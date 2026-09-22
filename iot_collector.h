/**
 * @file    iot_collector.h
 * @brief   多通道物联网数据采集与上报系统 IoT-Collector 统一头文件
 *          架构：三级流水线（接入→处理→分发） + 两级线程池
 */

/* 必须在任何系统头文件之前定义：
 * -std=c11 属于严格 ANSI 模式，glibc 不会暴露 struct ip_mreqn / IFNAMSIZ
 * 等扩展接口，组播相关代码会编译失败。 */
#define _DEFAULT_SOURCE 1
#ifndef IOT_COLLECTOR_H
#define IOT_COLLECTOR_H

#include <stdint.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>
#include <time.h>
#include <math.h>
#include <signal.h>
#include <unistd.h>
#include <pthread.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/epoll.h>
#include <sys/select.h>
#include <sys/time.h>
#include <syslog.h>
#include <netinet/in.h>
#include <netinet/ip.h>
#include <arpa/inet.h>
#include <net/if.h>
#include <fcntl.h>

/* =========================================================================
 * 一、编译期常量
 * ========================================================================= */

#define IOT_VERSION_STR         "1.0"

/* 队列 */
#define RINGQ_DEFAULT_CAP       4096u       /* 默认容量，必须为 2 的幂 */
#define RINGQ_BATCH_MAX         256u        /* 单次批量出队上限 */

/* 线程池 */
#define POOL_MIN_WORKERS        2u
#define POOL_MAX_WORKERS        16u
#define POOL_HIGH_WATER         0.80        /* 水位 >80% 触发扩容 */
#define POOL_LOW_WATER          0.30        /* 水位 <30% 触发缩容 */
#define POOL_HIGH_WATER_HOLD    3u          /* 连续 3 周期确认扩容 */
#define POOL_IDLE_RECLAIM_SEC   60          /* 空闲 >60s 回收 */
#define POOL_WATER_TICK_MS      1000        /* 水位采样周期 */

/* 令牌桶 */
#define TBF_DEFAULT_CPS         (512u  * 1024u)   /* 512 KB/s 出口限速 */
#define TBF_DEFAULT_BURST       (1024u * 1024u)   /* 1 MB 突发 */
#define TBF_TICK_MS             10u               /* 令牌补充时钟 */
#define TBF_MAX_FETCH_PER_OP    65536u            /* 单次最大取令牌字节数 */

/* 网络 */
#define INGRESS_EPOLL_MAX_EV    256
#define MCAST_RECV_GROUP        "239.10.10.10"
#define MCAST_SEND_GROUP        "239.20.20.20"
#define MCAST_TTL               16                /* 跨网段必须显式设置 */
#define MCAST_IFNAME            "eth0"
#define HEARTBEAT_INTERVAL_SEC  30                /* 心跳间隔 */
#define HEARTBEAT_LOST_LIMIT    3                 /* 连续失败判离线 */

/* 滑动窗口 */
#define WINDOW_SIZE             16
#define ANOMALY_SIGMA           3.0               /* 3σ 突变阈值 */

/* 统计 */
#define STATS_INTERVAL_SEC      1

/* 帧协议 */
#define FRAME_MAGIC_0           0xEB
#define FRAME_MAGIC_1           0x90

/* 错误码 */
typedef enum {
    IOT_OK              =  0,
    IOT_ERR_GENERIC     = -1,
    IOT_ERR_AGAIN       = -2,   /* 非阻塞模式暂不可用 */
    IOT_ERR_FULL        = -3,   /* 队列满 */
    IOT_ERR_EMPTY       = -4,   /* 队列空 */
    IOT_ERR_NOMEM       = -5,
    IOT_ERR_PROTO       = -6,   /* 协议/CRC 错误 */
    IOT_ERR_IO          = -7,
    IOT_ERR_TIMEOUT     = -8,
    IOT_ERR_SHUTDOWN    = -9,
} iot_status_t;

/* =========================================================================
 * 二、数据帧定义（跨主机协议）
 *  要点1：固定宽度类型，禁止裸 int / long
 *  要点2：__attribute__((packed)) 统一对齐
 *  要点3：多字节字段统一网络字节序
 * ========================================================================= */

typedef enum {
    FRAME_NORMAL = 0,   /* 普通数据帧 */
    FRAME_DATA  = 0,
    FRAME_ALARM  = 1,   /* 告警帧，永不丢弃 */
    FRAME_HEART  = 2,   /* 心跳帧 */
    FRAME_RETRY  = 3,   /* 补传帧 */
} frame_type_t;

/* 线程池控制任务标记：借用 env_frame_t 在队列里传递扩缩容指令。
 * 只在本进程内部使用，绝不会出现在线路上，因此不复用 frame_type_t。 */
#define FRAME_TYPE_CTRL         0xFF

typedef struct __attribute__((packed)) {
    uint8_t  magic[2];        /* 0xEB 0x90 帧头 */
    uint8_t  version;         /* 协议版本 */
    uint8_t  frame_type;      /* frame_type_t */
    uint16_t station_id;      /* 站点编号（网络序） */
    uint32_t timestamp_sec;   /* 采样时间戳-秒（网络序） */
    uint32_t timestamp_ms;    /* 采样时间戳-毫秒（网络序，0~999） */
    int16_t  temperature;     /* 温度，0.1℃ */
    uint16_t humidity;        /* 湿度，0.1% */
    uint16_t pm25;            /* PM2.5，μg/m³ */
    uint16_t pm10;            /* PM10，μg/m³ */
    uint16_t co_ppb;          /* CO，ppb */
    uint16_t noise_db;        /* 噪声，0.1dB */
    uint16_t crc16;           /* CRC16-CCITT，覆盖 magic 至 noise_db */
} env_frame_t;

#define ENV_FRAME_WIRE_SIZE     ((uint16_t)sizeof(env_frame_t))
#define ENV_FRAME_CRC_SIZE      ((uint16_t)(sizeof(env_frame_t) - sizeof(uint16_t)))

/* 计算 CRC16-CCITT 校验值 */
uint16_t crc16_ccitt(const uint8_t *data, size_t len);

/* 主机序 -> 网络序（发送前调用）。只转换业务字段，
 * 不含 crc16（它是派生的，由 frame_fill_crc 最后写入） */
void frame_hton(env_frame_t *f);

/* 网络序 -> 主机序（接收后调用，须在 CRC 校验通过之后） */
void frame_ntoh(env_frame_t *f);

/* 校验线路字节的帧头 magic 是否为 0xEB 0x90 */
int frame_magic_ok(const uint8_t *buf, size_t len);

/* 校验 CRC。入参必须是网络序帧（刚从线路拷出来的原始字节），
 * 即必须在 frame_ntoh 之前调用 */
int frame_crc_ok(const env_frame_t *frame);

/* 给网络序帧补写 crc16 字段，须在 frame_hton 之后调用 */
void frame_fill_crc(env_frame_t *frame);

/* 打包并补充 CRC，返回写入字节数，<0 表示失败 */
int frame_encode(const env_frame_t *in, uint8_t *buf, size_t buf_len);

/* 校验 CRC 并解包，返回 0 成功 */
int frame_decode(const uint8_t *buf, size_t len, env_frame_t *out);

/* =========================================================================
 * 三、有界环形队列（合并自 ring_queue.h，保留原版接口）
 * ========================================================================= */

typedef struct {
    env_frame_t     *buf;        /* 数据存储区（连续内存，容量 = capacity） */
    uint32_t         capacity;   /* 容量（必须是 2 的幂，用于位运算取模） */
    uint32_t         head;       /* 写指针（生产者写入位置） */
    uint32_t         tail;       /* 读指针（消费者读取位置） */
    uint32_t         count;      /* 当前有效元素数（head 与 tail 之间的元素个数） */
    uint32_t         dropped;    /* 统计：因满队丢弃的帧数 */

    pthread_mutex_t  lock;       /* 保护 buf / head / tail / count / dropped */
    pthread_cond_t   not_empty;  /* 消费者等待：队列非空（生产者入队后 signal） */
    pthread_cond_t   not_full;   /* 生产者等待：队列非满（消费者出队后 broadcast） */
} ring_queue_t;

/* 全局队列实例 */
extern ring_queue_t g_q1;
extern ring_queue_t g_q2;

/* 对外接口（原 ring_queue.h 签名） */
int      ring_init(ring_queue_t *q, uint32_t cap);
void     ring_destroy(ring_queue_t *q);
/* 阻塞入队：满时普通帧等待，告警帧挤占最旧普通帧 */
int      ring_push(ring_queue_t *q, const env_frame_t *frame);
/* 非阻塞入队（接入层/清洗层用）：满时普通帧丢弃，告警帧挤占；
 * 返回 IOT_OK / IOT_ERR_FULL */
int      ring_push_try(ring_queue_t *q, const env_frame_t *frame);
/* 批量出队：空则阻塞；g_running==0 且空时返回 0 */
uint32_t ring_pop_batch(ring_queue_t *q, env_frame_t *out, uint32_t max_n);
/* 退出唤醒：broadcast 两个条件变量 */
void     ring_wake_all(ring_queue_t *q);
/* 兼容旧名：与 ring_wake_all 相同 */
void     ring_queue_wakeup_all(ring_queue_t *q);
/* 当前有效元素个数（统计水位） */
uint32_t ring_count(ring_queue_t *q);

/* =========================================================================
 * 四、动态线程池（合并自 thread_pool.h，保留原版接口）
 * ========================================================================= */

typedef struct worker {
    pthread_t       tid;
    struct worker  *next;
    uint64_t        task_count;   /* 统计：处理任务数 */
    int             active;       /* 健康状态 */
} worker_t;

typedef struct thread_pool {
    worker_t           *workers;
    uint32_t            worker_num;
    uint32_t            max_workers;   /* 上限（根据 getrlimit RLIMIT_NPROC） */

    ring_queue_t       *task_queue;    /* 复用环形队列作为任务队列 */

    pthread_mutex_t     lock;
    pthread_cond_t      cond;
    int                 shutdown;

    /* 水位监控回调：队列使用率超过 80% 触发扩容，低于 30% 缩容 */
    void (*on_high_water)(struct thread_pool *);
    void (*on_low_water)(struct thread_pool *);
} thread_pool_t;

/* 任务函数签名 */
typedef void (*tp_task_fn)(void *arg);

/* 对外接口（原 thread_pool.h 签名） */
int  tp_init(thread_pool_t *tp, uint32_t min_w, uint32_t max_w, uint32_t qcap);
void tp_destroy(thread_pool_t *tp);
int  tp_submit(thread_pool_t *tp, tp_task_fn fn, void *arg);

/* =========================================================================
 * 五、令牌桶流量整形
 *  多桶实例：单播桶 / 补传桶 独立限速，互不影响
 * ========================================================================= */

typedef struct tbf {
    uint64_t         cps;              /* 每秒补充令牌数（字节） */
    uint64_t         burst;            /* 桶容量（字节） */
    uint64_t         tokens;           /* 当前令牌数 */

    pthread_mutex_t  lock;
    pthread_cond_t   cond;             /* 令牌不足时等待补充 */
    int              shutdown;
    uint64_t         throttled_bytes;  /* 累计限速字节统计 */
} tbf_t;

/* 初始化令牌桶，cps/burst 单位字节 */
int tbf_init(tbf_t **out, uint64_t cps, uint64_t burst);

/* 销毁令牌桶 */
void tbf_destroy(tbf_t *tbf);

/* 时钟线程每 10ms 调用，补充令牌并唤醒等待者 */
void tbf_tick(tbf_t *tbf, uint64_t elapsed_ms);

/* 取令牌：成功返回 0，超时/失败返回 <0；timeout_ms<0 表示无限等待 */
int tbf_fetch_token(tbf_t *tbf, uint64_t want, int timeout_ms);

/* 唤醒所有等待者并标记关闭（优雅退出用） */
void tbf_shutdown(tbf_t *tbf);

extern tbf_t *g_outbound_bucket;   /* 单播上报全局令牌桶 */
extern tbf_t *g_retry_bucket;      /* 断链补传令牌桶 */

/* 取令牌后发送，实现出口限速 */
int throttled_send(int fd, const void *buf, size_t len,
                   const struct sockaddr_in *dest, tbf_t *bucket);

/* =========================================================================
 * 六、站点上下文 / 滑动窗口滤波 (已修改为支持6参数)
 * ========================================================================= */

typedef struct
{
    uint16_t station_id;                 /* 站点编号，唯一标识 */

    /* 6个参数的滑动窗口 */
    int16_t  window_pm25[WINDOW_SIZE];   /* PM2.5 滑动窗口 */
    int16_t  window_pm10[WINDOW_SIZE];   /* PM10 滑动窗口 */
    int16_t  window_temp[WINDOW_SIZE];   /* 温度滑动窗口 */
    int16_t  window_hum[WINDOW_SIZE];    /* 湿度滑动窗口 */
    int16_t  window_co[WINDOW_SIZE];     /* CO 滑动窗口 */
    int16_t  window_noise[WINDOW_SIZE];  /* 噪声滑动窗口 */

    uint8_t  w_idx;                      /* 环形窗口写指针，6个参数共用 */
    uint8_t  w_count;                    /* 当前窗口内有效元素数量 */
} station_ctx_t;

/* 重置站点上下文 */
void station_ctx_reset(station_ctx_t *ctx, uint16_t station_id);

/* 批量统计：一次性算出6个参数的均值和标准差（顺序：pm25, pm10, temp, hum, co, noise） */
void window_stats_all(const station_ctx_t *ctx, double *means, double *sigmas);

/* 批量检测：一次性对6个参数进行 3σ 检测，返回 1 表示有异常，0 表示正常 */
int anomaly_detect_all(const station_ctx_t *ctx, const env_frame_t *frame);

typedef enum
{
    CLEAN_PASS    = 0,   /* 通过 */
    CLEAN_DROP    = 1,   /* 丢弃（重复/异常） */
    CLEAN_ALARM   = 2,   /* 转告警帧 */
} clean_result_t;

/* 数据清洗：范围校验 + 突变检测 + 乱序重排 */
clean_result_t cleaner_process(env_frame_t *frame);

/* 清洗层初始化与销毁 */
void cleaner_init(void);
void cleaner_destroy(void);

/* =========================================================================
 * 七、网络通信
 * ========================================================================= */

/* ---- 流式解码：把 TCP 字节流切成完整帧（解决粘包 / 拆包） ---- */

typedef struct {
    uint8_t  buf[ENV_FRAME_WIRE_SIZE * 4u];  /* 累积缓冲，够放下若干帧 */
    size_t   len;                            /* 当前有效字节数 */
    uint64_t resync;                         /* 丢弃无效字节重新同步的次数 */
} frame_stream_t;

/* 每解出一帧就回调一次，回调里再决定往哪塞 */
typedef void (*frame_sink_fn)(const env_frame_t *frame, void *arg);

/* 清空累积缓冲（连接重建时调用） */
void frame_stream_reset(frame_stream_t *st);

/* 追加一段字节流，内部按帧长切分并逐帧回调。
 * 返回本次解析出的帧数；剩余不足一帧的字节留在缓冲里等下次 */
int frame_stream_feed(frame_stream_t *st, const uint8_t *data, size_t n,
                      frame_sink_fn sink, void *arg);

/* 解码线路字节为帧：校验 magic + CRC，输出主机序帧。返回 0 成功 */
int decode_wire(const uint8_t *buf, size_t len, env_frame_t *out);

/* ---- UDP 接入：单播 + 组播，共用一个 epoll 实例 ---- */

typedef struct {
    uint16_t      ucast_port;     /* UDP 单播监听端口，0 表示不启用 */
    const char   *mcast_group;    /* 组播组地址，NULL 表示不启用组播接收 */
    uint16_t      mcast_port;     /* 组播监听端口 */
    const char   *mcast_ifname;   /* 加入组播的网卡名，NULL 交给系统默认路由 */
    ring_queue_t *q1;             /* 出队目标：Q1（必需） */
} udp_ingress_cfg_t;

/* 启动 UDP 接收线程（单播与组播都走同一个 epoll 循环） */
int  udp_receiver_start(const udp_ingress_cfg_t *cfg, volatile sig_atomic_t *running);

/* 请求退出：关闭 socket 让 epoll 立即返回 */
void udp_receiver_stop(void);

/* 等待接收线程结束 */
int  udp_receiver_join(void);

/* 初始化 UDP 单播接收 socket，端口 port，返回 fd */
int udp_ucast_init(uint16_t port, struct sockaddr_in *out_addr);

/* 初始化 UDP 组播接收：bind + IP_ADD_MEMBERSHIP，返回 fd */
int udp_mcast_init(const char *group, uint16_t port, const char *ifname,
                   struct sockaddr_in *out_bind);

/* ---- TCP 主动采集（旧设备兼容） ---- */

typedef struct {
    const char   *dev_ip;            /* 旧设备 IP（必需） */
    uint16_t      dev_port;          /* 旧设备端口 */
    uint32_t      poll_interval_ms;  /* 每轮采集后的间隔，0 取默认 200ms */
    ring_queue_t *q1;                /* 出队目标：Q1（必需） */
} tcp_poller_cfg_t;

/* 启动 TCP 采集线程：长连接 + 流式解码，断链按退避重连 */
int  tcp_poller_start(const tcp_poller_cfg_t *cfg, volatile sig_atomic_t *running);

/* 请求采集线程退出 */
void tcp_poller_stop(void);

/* 等待采集线程结束 */
int  tcp_poller_join(void);

/* 初始化组播发送：IP_MULTICAST_IF + IP_MULTICAST_TTL */
int mcast_sender_init(const char *group, uint16_t port, const char *ifname,
                      uint8_t ttl);

/* 组播发送一帧数据 */
int mcast_sender_send(int fd, const void *buf, size_t len);

/* 初始化单播发送：与监管平台建立 TCP 长连接 */
int unicast_sender_init(const char *ip, uint16_t port);

/* 单播发送一帧数据（走 g_outbound_bucket 限速） */
int unicast_sender_send(int fd, const void *buf, size_t len);

/* 用指定令牌桶限速发送：补传走独立桶，不挤占实时上报的带宽 */
int unicast_sender_send_with_bucket(int fd, const void *buf, size_t len,
                                    tbf_t *bucket);

/* 发送应用层心跳帧（30s 间隔） */
int unicast_sender_heartbeat(int fd);

/* 关闭单播连接 */
void unicast_sender_close(int fd);

/* 返回监管平台链路状态：1 在线，0 离线 */
int unicast_link_online(void);

/* 返回当前连续发送失败次数（达到 HEARTBEAT_LOST_LIMIT 即判离线） */
uint32_t unicast_heartbeat_lost(void);

/* 打开断链缓存文件（带游标） */
int cache_writer_open(const char *path);

/* 追加一帧到本地缓存（多分发线程并发调用，内部加锁） */
int cache_writer_append(const env_frame_t *frame);

/* 将缓存刷盘 fsync */
int cache_writer_flush(void);

/* 关闭缓存文件 */
int cache_writer_close(void);

/* 恢复后按序补传：从读游标一直补到写游标，无重复无丢失。
 * fd 为已连通的监管平台连接，补传帧打上 FRAME_RETRY 标记，
 * 并走 g_retry_bucket 独立限速 */
int cache_replay_start(int fd);

/* 返回尚未补传的帧数（缓存积压量） */
uint64_t cache_pending_frames(void);

/* =========================================================================
 * 七点五、分发与限速层（egress）
 *  从 Q2 取已清洗的帧 → 组播分发（不限速）+ 单播上报（令牌桶限速）；
 *  单播链路断开时写本地缓存，链路恢复后触发补传。
 *  本层只依赖 ring_queue 的 ring_pop_batch()，不关心数据从哪来。
 * ========================================================================= */

typedef struct {
    ring_queue_t *q2;             /* 数据源：Q2 环形队列（必需） */
    uint32_t      worker_num;     /* 分发线程数，0 则取 POOL_MIN_WORKERS */
    const char   *mcast_group;    /* 组播组地址，NULL 表示不启用组播分发 */
    uint16_t      mcast_port;     /* 组播目的端口 */
    const char   *mcast_ifname;   /* 组播出口网卡名，NULL 表示交给系统默认路由 */
    uint8_t       mcast_ttl;      /* 组播 TTL，0 则取 MCAST_TTL */
    const char   *platform_ip;    /* 监管平台 IP，NULL 表示不启用单播上报 */
    uint16_t      platform_port;  /* 监管平台端口 */
    const char   *cache_path;     /* 断链缓存文件路径，NULL 表示不落盘 */
} dispatcher_cfg_t;

/* 启动分发层：初始化出口与令牌桶，创建 worker_num 个分发线程 + 1 个监控线程 */
int  dispatcher_start(const dispatcher_cfg_t *cfg, volatile sig_atomic_t *running);

/* 请求退出：唤醒 Q2，让阻塞在取数据上的分发线程返回 */
void dispatcher_stop(void);

/* 等待所有分发线程结束，并释放出口资源 */
int  dispatcher_join(void);

/* 返回当前分发线程数（供统计报表使用） */
uint32_t dispatcher_worker_count(void);

/* =========================================================================
 * 八、运行统计
 * ========================================================================= */

typedef struct {
    /* 接入层 */
    uint64_t udp_ucast_frames;
    uint64_t udp_mcast_frames;
    uint64_t tcp_frames;
    uint64_t sim_frames;
    uint64_t ingress_total;
    uint64_t decode_fail;         /* magic/CRC 校验失败帧数 */

    /* 清洗层 */
    uint64_t cleaned_frames;      /* 通过清洗进入 Q2 */
    uint64_t anomaly_frames;      /* 判定为突变、转告警帧 */
    uint64_t range_drop;          /* 越界丢弃 */
    uint64_t dup_drop;            /* 重复丢弃 */
    uint64_t reorder_drop;        /* 乱序丢弃 */

    /* 队列 */
    uint32_t q1_count;
    uint32_t q1_cap;
    uint32_t q2_count;
    uint32_t q2_cap;
    uint64_t dropped_total;

    /* 线程池 */
    uint32_t parser_workers;
    uint32_t parser_busy;
    uint32_t dispatcher_workers;
    uint32_t dispatcher_busy;

    /* 出口 */
    uint64_t mcast_ok;
    uint64_t unicast_ok;
    uint64_t cache_pending;       /* 断链缓存积压帧数 */
    uint64_t throttled_bytes;

    /* 链路 */
    int      platform_link_up;    /* 1 = ESTABLISHED */
    uint32_t heartbeat_lost;      /* 当前连续失败次数 */
} iot_stats_t;

/* 初始化统计模块 */
void stats_init(void);

/* 绑定 Q1/Q2，让快照与周期报表能带出队列水位（可传 NULL 表示不统计） */
void stats_bind_queues(ring_queue_t *q1, ring_queue_t *q2);

/* 记录接入层收帧：proto 0=ucast 1=mcast 2=tcp 3=sim */
void stats_on_ingress(int proto, uint32_t n);

/* 记录解码失败帧数（magic/CRC 错误） */
void stats_on_decode_fail(uint32_t n);

/* 记录清洗结果：kind 0=pass 1=alarm 2=range_drop 3=dup_drop 4=reorder_drop */
void stats_on_clean(int kind, uint32_t n);

/* 记录丢帧数 */
void stats_on_drop(uint32_t n);

/* 记录出口发帧：channel 0=mcast 1=unicast */
void stats_on_egress(int channel, uint32_t n);

/* 记录限速字节数 */
void stats_on_throttled(uint64_t bytes);

/* 获取统计快照 */
void stats_snapshot(iot_stats_t *out);

/* 刷新「运行态」字段（线程数 / 忙线程数 / 链路状态）。
 * 这些值不在本模块维护，由主控每秒采集后写进来，供报表统一输出 */
void stats_update_runtime(uint32_t parser_workers, uint32_t parser_busy,
                          uint32_t dispatcher_workers, uint32_t dispatcher_busy,
                          int platform_link_up, uint32_t heartbeat_lost);

/* 每秒输出统计到 syslog（DEBUG 级） */
void stats_report(void);

/* =========================================================================
 * 九、信号与优雅退出
 * ========================================================================= */

extern volatile sig_atomic_t g_running;

/* 注册 SIGINT/SIGTERM 处理（置 g_running=0），忽略 SIGPIPE */
int iot_signal_init(void);

/* 优雅退出序列：唤醒 -> join -> 冲刷 Q2 -> 关闭 socket -> syslog */
void iot_shutdown_sequence(void);

/* 日志宏，统一前缀 [IoT-Collector] */
#define IOT_LOG(prio, fmt, ...) \
    syslog(prio, "[IoT-Collector] " fmt, ##__VA_ARGS__)

#endif /* IOT_COLLECTOR_H */
