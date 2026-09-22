//TCP旧设备采集 — 长连接 + 流式切帧 + 超时/假错 + 协作退出
//  1) 客户端通常不 bind;IP 用 inet_pton,端口用 htons
//  2) connect 用非阻塞 + select 写集合超时,避免不可达时卡住退出
//  3) 收包走 frame_stream_feed: 解决TCP半包/粘包(头文件流式解码接口)
//  4) 假错 EINTR/EAGAIN/EINPROGRESS: continue;真错/对端关闭: 断链退避重连
//  5) 入队 ring_push_try,统计 stats_on_ingress(2=tcp)
//  6) 退出: g_running=0 → stop 关 g_fd → 分片退避可中断 → join
#include "iot_collector.h"//引入项目总头文件

static pthread_t g_tid;//采集线程id
static int g_thread_created = 0;//线程是否已创建
static volatile sig_atomic_t *g_run_flag = NULL;//运行标志指针
static ring_queue_t *g_push_q1 = NULL;//入队目标Q1(指针,避免与全局队列对象 g_q1 重名)
static char g_dev_ip[64];//旧设备IP
static uint16_t g_dev_port = 0;//旧设备端口
static uint32_t g_poll_ms = 200;//每轮采集间隔,0按默认200ms
static int g_fail_times = 0;//连续失败次数(退避)
static int g_fd = -1;//当前连接fd(文件作用域,stop可close)
static frame_stream_t g_stream;//TCP流式切帧缓冲

//是否仍在运行
static int tcp_running(void)
{
    return (g_run_flag != NULL && (*g_run_flag) != 0);
}

//day07: 退避/间隔按100ms分片,随时响应退出标志
static void backoff_sleep_ms(int ms)
{
    int steps = ms / 100;//分片数
    int i = 0;
    if (steps < 1) {//至少一片
        steps = 1;
    }
    for (i = 0; i < steps && tcp_running(); i++) {//退出则立刻结束
        usleep(100000);//100ms
    }
}

//流式解码回调: 一帧主机序数据 → try-push 入Q1
static void tcp_frame_sink(const env_frame_t *frame, void *arg)
{
    (void)arg;//未使用
    if (frame == NULL || g_push_q1 == NULL) {//判空
        return;
    }
    if (ring_push_try(g_push_q1, frame) == IOT_OK) {//非阻塞入队(§4.2)
        stats_on_ingress(2, 1);//proto 2 = tcp
    } else {
        stats_on_drop(1);//队列满
    }
}

/*
 * 带超时的TCP连接
 * 成功返回已连接fd,失败返回-1并已close
 */
static int tcp_connect_timeout(const char *ip, uint16_t port, int timeout_ms)
{
    int fd = -1;//套接字
    int flags = 0;//原文件状态
    int ret = 0;//connect/select返回
    int soerr = 0;//SO_ERROR
    socklen_t soerr_len = sizeof(soerr);
    struct sockaddr_in addr;//对端
    struct timeval tv;//超时
    fd_set wset;//写集合

    fd = socket(AF_INET, SOCK_STREAM, 0);//TCP流式套接字
    if (fd < 0) {
        perror("socket(tcp)");
        return -1;
    }
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;//IPv4
    addr.sin_port = htons(port);//端口网络序
    if (inet_pton(AF_INET, ip, &addr.sin_addr) <= 0) {//点分十进制→网络格式
        fprintf(stderr, "inet_pton 无效 IP : %s\n", ip);
        close(fd);
        return -1;
    }

    flags = fcntl(fd, F_GETFL, 0);//取标志
    if (flags < 0 || fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0) {//非阻塞
        perror("fcntl(O_NONBLOCK)");
        close(fd);
        return -1;
    }

    ret = connect(fd, (struct sockaddr *)&addr, sizeof(addr));//三次握手
    if (ret == 0) {
        //本机立即成功
    } else if (errno == EINPROGRESS) {//假错:连接进行中
        FD_ZERO(&wset);//清空(select会改集合,每次重设)
        FD_SET(fd, &wset);//监听可写
        tv.tv_sec = timeout_ms / 1000;
        tv.tv_usec = (timeout_ms % 1000) * 1000;
        ret = select(fd + 1, NULL, &wset, NULL, &tv);//等待就绪
        if (ret < 0) {//select失败
            if (errno != EINTR) {
                perror("select(connect)");
            }
            close(fd);
            return -1;
        }
        if (ret == 0) {//超时
            close(fd);
            errno = ETIMEDOUT;
            return -1;
        }
        if (getsockopt(fd, SOL_SOCKET, SO_ERROR, &soerr, &soerr_len) < 0 || soerr != 0) {
            //可写≠连接成功,必须SO_ERROR确认
            if (soerr != 0) {
                errno = soerr;
            }
            close(fd);
            return -1;
        }
    } else {//真错
        close(fd);
        return -1;
    }

    //恢复阻塞,配合SO_RCVTIMEO做可控recv
    if (fcntl(fd, F_SETFL, flags) < 0) {
        perror("fcntl(restore)");
        close(fd);
        return -1;
    }
    tv.tv_sec = 0;
    tv.tv_usec = 400000;//接收超时400ms
    if (setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)) < 0) {
        perror("setsockopt(SO_RCVTIMEO)");
    }
    return fd;//已连接
}

//单轮长连接会话: connect → 循环recv+流式解码 → 断链返回
static int tcp_session_once(void)
{
    uint8_t buf[512];//收包缓冲区
    int got = 0;//本轮成功入队帧数(经sink统计,这里记回调次数不便,用流返回值累加)
    int fd = -1;

    if (!tcp_running()) {//已要求退出
        return -1;
    }
    fd = tcp_connect_timeout(g_dev_ip, g_dev_port, 400);//connect超时400ms
    if (fd < 0) {//连接失败
        return -1;
    }
    g_fd = fd;//暴露给stop打断
    frame_stream_reset(&g_stream);//新连接重置切帧缓冲

    while (tcp_running()) {//长连接收数
        ssize_t n = recv(fd, buf, sizeof(buf), 0);//读字节流
        if (n < 0) {//错误
            if (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK) {//假错
                continue;//重试
            }
            break;//真错→断链重连
        }
        if (n == 0) {//对端关闭
            break;
        }
        //流式切帧并回调入队
        got += frame_stream_feed(&g_stream, buf, (size_t)n, tcp_frame_sink, NULL);
    }

    if (g_fd == fd) {//仍是本连接
        close(fd);//关闭连接
        g_fd = -1;
    }
    return (got > 0) ? 0 : -1;//本轮有解出帧算成功
}

//采集线程入口: 长连接会话 + 断链退避重连
static void *tcp_poll_thread(void *arg)
{
    (void)arg;
    while (tcp_running()) {//协作退出
        if (tcp_session_once() != 0) {//本轮失败(连不上或无数据)
            g_fail_times++;//失败+1
            if (g_fail_times < 5) {//递增退避
                backoff_sleep_ms((int)g_fail_times * 1000);
            } else {//上限5s
                backoff_sleep_ms(5000);
            }
        } else {//成功
            g_fail_times = 0;//清零
            //会话正常结束(对端关)后按 poll_interval 稍歇再连
            backoff_sleep_ms((g_poll_ms > 0) ? (int)g_poll_ms : 200);
        }
    }
    return NULL;
}

//启动主接口: 按 tcp_poller_cfg_t 启动采集线程
int tcp_poller_start(const tcp_poller_cfg_t *cfg, volatile sig_atomic_t *running)
{
    int ret = 0;//pthread返回值

    if (cfg == NULL || cfg->dev_ip == NULL || cfg->dev_port == 0 ||
        cfg->q1 == NULL || running == NULL) {//参数校验
        return IOT_ERR_GENERIC;
    }
    snprintf(g_dev_ip, sizeof(g_dev_ip), "%s", cfg->dev_ip);//记录设备IP
    g_dev_port = cfg->dev_port;//端口
    g_poll_ms = (cfg->poll_interval_ms > 0) ? cfg->poll_interval_ms : 200;//采集间隔
    g_push_q1 = cfg->q1;//目标队列
    g_run_flag = running;//运行标志
    g_fail_times = 0;
    frame_stream_reset(&g_stream);//清空流缓冲

    ret = pthread_create(&g_tid, NULL, tcp_poll_thread, NULL);//创建线程
    if (ret != 0) {
        fprintf(stderr, "pthread_create(tcp) : %s\n", strerror(ret));
        return IOT_ERR_GENERIC;
    }
    g_thread_created = 1;
    return IOT_OK;
}

//停止接口: 关闭当前连接fd,打断阻塞中的 recv/select
void tcp_poller_stop(void)
{
    if (g_fd >= 0) {//仍有连接
        close(g_fd);//让阻塞IO尽快返回
        g_fd = -1;
    }
}

//等待线程结束
int tcp_poller_join(void)
{
    if (g_thread_created == 0) {
        return IOT_OK;
    }
    pthread_join(g_tid, NULL);
    g_thread_created = 0;
    return IOT_OK;
}
