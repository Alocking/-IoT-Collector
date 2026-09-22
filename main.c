//主控/生命周期 — 六要点
//  1) 数据流：接入层 → Q1 → 清洗线程 → Q2 → 分发层 → 组播/单播/缓存
//  2) Q1/Q2 容量 4096=2^12，配合位运算取模
//  3) 入队接口统一 ring_init/ring_push_try/ring_pop_batch/ring_wake_all
//  4) 清洗线程批量 pop → cleaner_process → try-push 入 Q2
//  5) 主循环每秒刷新 stats_update_runtime（线程数/链路状态）
//  6) Ctrl+C：g_running=0 → 各层 stop/join → 销毁队列
#include "iot_collector.h"//引入项目总头文件
#include <getopt.h>//命令行解析

/* ======================= 默认配置 ======================= */
#define DEFAULT_UDP_PORT        6000
#define DEFAULT_TCP_IP          "127.0.0.1"
#define DEFAULT_TCP_PORT        7000
/* 收发组地址必须分离：
 * 若 ingress 与 egress 共用同一组，本机发送的组播会被自己再采回，
 * 形成 Q1→清洗→Q2→组播→Q1 的回环放大 */
#define DEFAULT_MCAST_RECV      MCAST_RECV_GROUP   /* 239.10.10.10 接入监听 */
#define DEFAULT_MCAST_SEND      MCAST_SEND_GROUP   /* 239.20.20.20 分发出口 */
#define DEFAULT_MCAST_RECV_PORT 6001
#define DEFAULT_MCAST_SEND_PORT 6002
#define DEFAULT_PLATFORM_IP     "127.0.0.1"
#define DEFAULT_PLATFORM_PORT   8000
#define DEFAULT_CACHE_PATH      "./cache.dat"

/* ======================= 全局队列（头文件 extern） ======================= */
ring_queue_t g_q1;   /* 原始数据队列：接入层 -> 清洗层 */
ring_queue_t g_q2;   /* 清洗后队列：清洗层 -> 分发层 */

/* 内置模拟开关：--sim 时造帧入 Q1，便于无网卡演示 */
static int g_opt_sim = 0;

/* ======================= 内置模拟线程 ======================= */
/**
 * @brief 模拟接入：周期性生成站点帧推入 Q1（仅 --sim 使用）
 *        对应 README「内置 sim 线程」；真实部署用 station_sim 外部发包。
 */
static void *sim_thread(void *arg)
{
    volatile sig_atomic_t *run = (volatile sig_atomic_t *)arg;//运行标志
    uint16_t station = 1;//站点编号起点
    uint32_t seq = 0;//序号

    while (run != NULL && (*run) != 0) {//协作退出
        env_frame_t f;//主机序帧
        memset(&f, 0, sizeof(f));//清零
        f.magic[0] = FRAME_MAGIC_0;//帧头
        f.magic[1] = FRAME_MAGIC_1;
        f.version = 1;//协议版本
        f.frame_type = FRAME_NORMAL;//普通数据帧
        f.station_id = station;//站点
        f.timestamp_sec = (uint32_t)time(NULL);//当前秒
        f.timestamp_ms = (uint32_t)(seq % 1000);//伪毫秒
        f.temperature = (int16_t)(200 + (seq % 50));//20.0℃ 附近
        f.humidity = (uint16_t)(500 + (seq % 30));//50% 附近
        f.pm25 = (uint16_t)(30 + (seq % 20));//正常区间
        f.pm10 = (uint16_t)(50 + (seq % 20));
        f.co_ppb = (uint16_t)(200 + (seq % 10));
        f.noise_db = (uint16_t)(400 + (seq % 20));
        /* 偶发尖峰，便于观察 3σ（窗口满后 anomaly 会涨） */
        if ((seq % 17u) == 0u) {
            f.pm25 = 800;
        }

        if (ring_push_try(&g_q1, &f) == IOT_OK) {//非阻塞入 Q1
            stats_on_ingress(3, 1);//统计 sim 接入
        } else {
            stats_on_drop(1);//队列满丢弃
        }

        seq++;//序号推进
        if ((seq % 20u) == 0u) {//每 20 帧换一个站点
            station = (uint16_t)(station % 5u + 1u);//1~5 轮转
        }
        usleep(20000);//约 50fps 总量，足够观察统计
    }
    return NULL;//线程结束
}

/* ======================= 清洗线程 ======================= */
/**
 * @brief 清洗线程入口
 *        从 Q1 批量取原始帧，调用 cleaner_process 清洗，
 *        将通过（CLEAN_PASS / CLEAN_ALARM）的帧推入 Q2。
 *        知识点：生产者-消费者 + 条件变量
 */
static void *cleaner_thread(void *arg)
{
    (void)arg;//未使用参数
    env_frame_t batch[RINGQ_BATCH_MAX];//批量缓冲

    while (g_running) {//运行标志协作退出
        /* 批量出队：空则在条件变量上等待；g_running=0 时返回 0 */
        uint32_t n = ring_pop_batch(&g_q1, batch, RINGQ_BATCH_MAX);
        if (n == 0) {
            if (!g_running) break;//收到退出信号
            continue;//短暂无数据，继续等
        }

        for (uint32_t i = 0; i < n; i++) {//逐帧清洗
            clean_result_t res = cleaner_process(&batch[i]);//清洗核心
            if (res == CLEAN_PASS || res == CLEAN_ALARM) {//通过/告警
                stats_on_clean(res == CLEAN_PASS ? 0 : 1, 1);//清洗统计
                /* try-push：Q2 满不阻塞清洗线程，丢帧计入统计（§4.2） */
                if (ring_push_try(&g_q2, &batch[i]) != IOT_OK) {
                    stats_on_drop(1);//丢帧
                }
            }
            /* CLEAN_DROP 的分类统计由 cleaner_process 内部完成 */
        }
    }
    return NULL;//线程返回
}

/* ======================= 用法提示 ======================= */
static void print_usage(const char *prog)
{
    fprintf(stderr,
        "Usage: %s [options]\n"
        "  -u <port>      UDP 单播监听端口 (默认: %d)\n"
        "  -t <ip:port>   TCP 采集目标地址 (默认: %s:%d)\n"
        "  -m <group>     组播接入组 (默认: %s)\n"
        "  -M <port>      组播接入端口 (默认: %d)\n"
        "  -r <group>     组播分发组 (默认: %s，须与接入组不同)\n"
        "  -R <port>      组播分发端口 (默认: %d)\n"
        "  -p <ip:port>   监管平台单播上报地址 (默认: %s:%d)\n"
        "  -c <path>      断链缓存文件路径 (默认: %s)\n"
        "  -s             内置模拟器（无外部发包也可跑通流水线）\n"
        "  -h             显示本帮助\n",
        prog,
        DEFAULT_UDP_PORT,
        DEFAULT_TCP_IP, DEFAULT_TCP_PORT,
        DEFAULT_MCAST_RECV,
        DEFAULT_MCAST_RECV_PORT,
        DEFAULT_MCAST_SEND,
        DEFAULT_MCAST_SEND_PORT,
        DEFAULT_PLATFORM_IP, DEFAULT_PLATFORM_PORT,
        DEFAULT_CACHE_PATH);
}

/* ======================= 主函数 ======================= */
int main(int argc, char *argv[])
{
    /* ---------- 命令行参数 ---------- */
    uint16_t udp_port       = DEFAULT_UDP_PORT;//UDP 单播端口
    char     tcp_ip[64]     = DEFAULT_TCP_IP;//TCP 目标 IP
    uint16_t tcp_port       = DEFAULT_TCP_PORT;//TCP 端口
    char     mcast_recv[64] = DEFAULT_MCAST_RECV;//组播接入组
    uint16_t mcast_rport    = DEFAULT_MCAST_RECV_PORT;//组播接入端口
    char     mcast_send[64] = DEFAULT_MCAST_SEND;//组播分发组
    uint16_t mcast_sport    = DEFAULT_MCAST_SEND_PORT;//组播分发端口
    char     platform_ip[64]= DEFAULT_PLATFORM_IP;//监管平台 IP
    uint16_t platform_port  = DEFAULT_PLATFORM_PORT;//监管平台端口
    char     cache_path[256]= DEFAULT_CACHE_PATH;//断链缓存路径

    int opt;
    while ((opt = getopt(argc, argv, "u:t:m:M:r:R:p:c:sh")) != -1) {//解析参数
        switch (opt) {
        case 'u'://UDP 端口
            udp_port = (uint16_t)atoi(optarg);//字符串转端口
            break;
        case 't': {//TCP ip:port
            char *colon = strchr(optarg, ':');//查找冒号
            if (colon) {
                *colon = '\0';//切开
                strncpy(tcp_ip, optarg, sizeof(tcp_ip) - 1);//拷 IP
                tcp_port = (uint16_t)atoi(colon + 1);//取端口
            }
            break;
        }
        case 'm'://组播接入组（ingress listen）
            strncpy(mcast_recv, optarg, sizeof(mcast_recv) - 1);
            break;
        case 'M'://组播接入端口
            mcast_rport = (uint16_t)atoi(optarg);
            break;
        case 'r'://组播分发组（egress send）
            strncpy(mcast_send, optarg, sizeof(mcast_send) - 1);
            break;
        case 'R'://组播分发端口
            mcast_sport = (uint16_t)atoi(optarg);
            break;
        case 'p': {//平台 ip:port
            char *colon = strchr(optarg, ':');//查找冒号
            if (colon) {
                *colon = '\0';//切开
                strncpy(platform_ip, optarg, sizeof(platform_ip) - 1);
                platform_port = (uint16_t)atoi(colon + 1);
            }
            break;
        }
        case 'c'://缓存路径
            strncpy(cache_path, optarg, sizeof(cache_path) - 1);
            break;
        case 's'://内置模拟
            g_opt_sim = 1;//置位
            break;
        case 'h'://帮助
        default:
            print_usage(argv[0]);//打印用法
            return 1;//退出
        }
    }

    openlog("iot-collector", LOG_PID | LOG_CONS, LOG_USER);//syslog 初始化

    /* ---------- 1. 初始化信号处理 ---------- */
    if (iot_signal_init() != 0) {//注册 SIGINT/SIGTERM，忽略 SIGPIPE
        fprintf(stderr, "Failed to init signal handler\n");
        return 1;
    }

    /* ---------- 2. 初始化统计模块 ---------- */
    stats_init();//清零计数并启动统计线程

    /* ---------- 3. 初始化队列 Q1 / Q2（容量须为 2 的幂） ---------- */
    if (ring_init(&g_q1, RINGQ_DEFAULT_CAP) != 0) {//Q1 原始帧
        fprintf(stderr, "Failed to init Q1\n");
        return 1;
    }
    if (ring_init(&g_q2, RINGQ_DEFAULT_CAP) != 0) {//Q2 清洗后
        fprintf(stderr, "Failed to init Q2\n");
        ring_destroy(&g_q1);//回滚 Q1
        return 1;
    }
    stats_bind_queues(&g_q1, &g_q2);//绑定水位统计

    /* ---------- 4. 初始化清洗层（站点上下文表） ---------- */
    cleaner_init();//清空站点上下文

    /* ---------- 5. 启动接入层 ---------- */
    /* 5.1 UDP 接收 */
    udp_ingress_cfg_t udp_cfg = {
        .ucast_port  = udp_port,
        .mcast_group = mcast_recv,   /* 接入组，与分发组分离，防回环 */
        .mcast_port  = mcast_rport,
        .mcast_ifname= NULL,          /* 使用系统默认路由 */
        .q1          = &g_q1
    };
    if (udp_receiver_start(&udp_cfg, &g_running) != 0) {
        fprintf(stderr, "Failed to start UDP receiver\n");
        /* 不致命，继续尝试其他通道 */
    }

    /* 5.2 TCP 主动采集（旧设备兼容，流式切帧） */
    tcp_poller_cfg_t tcp_cfg = {
        .dev_ip          = tcp_ip,
        .dev_port        = tcp_port,
        .poll_interval_ms= 200,
        .q1              = &g_q1
    };
    if (tcp_poller_start(&tcp_cfg, &g_running) != 0) {
        fprintf(stderr, "Failed to start TCP poller\n");//无设备时可忽略
    }

    /* ---------- 6. 启动清洗线程 ---------- */
    pthread_t cleaner_tid;
    if (pthread_create(&cleaner_tid, NULL, cleaner_thread, NULL) != 0) {
        fprintf(stderr, "Failed to create cleaner thread\n");
        udp_receiver_stop();//回滚接入层
        tcp_poller_stop();
        udp_receiver_join();
        tcp_poller_join();
        cleaner_destroy();
        ring_destroy(&g_q1);
        ring_destroy(&g_q2);
        return 1;
    }

    /* ---------- 6.5 可选：内置模拟线程 ---------- */
    pthread_t sim_tid;
    int sim_on = 0;//模拟线程是否创建
    if (g_opt_sim) {//用户指定了 -s
        if (pthread_create(&sim_tid, NULL, sim_thread, (void *)&g_running) == 0) {
            sim_on = 1;//创建成功
            printf("[IoT-Collector] builtin simulator enabled\n");
        }
    }

    /* ---------- 7. 启动分发层（Q2 → 组播/单播/缓存） ---------- */
    dispatcher_cfg_t disp_cfg = {
        .q2             = &g_q2,
        .worker_num     = POOL_MIN_WORKERS,
        .mcast_group    = mcast_send,  /* 分发组 ≠ 接入组 */
        .mcast_port     = mcast_sport,
        .mcast_ifname   = NULL,
        .mcast_ttl      = MCAST_TTL,
        .platform_ip    = platform_ip,
        .platform_port  = platform_port,
        .cache_path     = cache_path
    };
    if (dispatcher_start(&disp_cfg, &g_running) != 0) {
        fprintf(stderr, "Failed to start dispatcher\n");
        g_running = 0;//通知退出
        ring_wake_all(&g_q1);//唤醒清洗线程
        pthread_join(cleaner_tid, NULL);
        if (sim_on) {
            pthread_join(sim_tid, NULL);
        }
        udp_receiver_stop();
        tcp_poller_stop();
        udp_receiver_join();
        tcp_poller_join();
        cleaner_destroy();
        ring_destroy(&g_q1);
        ring_destroy(&g_q2);
        return 1;
    }

    /* ---------- 8. 主循环：每秒刷新运行时统计 ---------- */
    while (g_running) {//直到信号置 0
        stats_update_runtime(
            0, 0,                                   /* parser_workers, parser_busy */
            dispatcher_worker_count(), 0,           /* dispatcher_workers, dispatcher_busy */
            unicast_link_online(),                  /* 平台链路 1/0 */
            unicast_heartbeat_lost()                /* 连续失败次数 */
        );
        sleep(1);//每秒一次
    }

    /* ---------- 9. 优雅退出 ---------- */
    printf("Shutting down...\n");

    /* 9.1 停止接入层 */
    udp_receiver_stop();//关 socket 使 epoll 返回
    tcp_poller_stop();//关连接使采集线程退出

    /* 9.2 等待接入层线程结束 */
    udp_receiver_join();
    tcp_poller_join();
    if (sim_on) {//模拟线程靠 g_running 退出
        pthread_join(sim_tid, NULL);
    }

    /* 9.3 唤醒 Q1，让清洗线程退出 */
    ring_wake_all(&g_q1);//broadcast 两个条件变量
    pthread_join(cleaner_tid, NULL);//回收清洗线程

    /* 9.4 停止分发层（内部冲刷 Q2 并释放出口资源） */
    dispatcher_stop();
    dispatcher_join();

    /* 9.5 销毁清洗层站点上下文 */
    cleaner_destroy();

    /* 9.6 销毁队列 */
    ring_destroy(&g_q1);
    ring_destroy(&g_q2);

    stats_report();//最终一次统计
    closelog();//关闭 syslog
    printf("Shutdown complete.\n");
    return 0;//正常退出
}
