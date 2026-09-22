//单播上报出口 — 五要点
//  1) connect：非阻塞 + poll 超时，假错 EINPROGRESS 用 SO_ERROR 复核
//  2) send：先 tbf_fetch_token 再写，实现出口限速 512KB/s（FR-05）
//  3) 多线程共用一条 TCP：必须 g_send_lock 串行化，否则字节交错无法解析
//  4) 心跳帧不走令牌桶，避免数据积压拖垮链路活性判定
//  5) 连续 HEARTBEAT_LOST_LIMIT 次失败判离线，交由 dispatcher 重连
#include "iot_collector.h"//引入项目总头文件

#include <fcntl.h>
#include <poll.h>
#include <netinet/tcp.h>

#define UNICAST_CONNECT_TIMEOUT_MS   3000   /* 建链超时 */
#define UNICAST_SEND_TIMEOUT_MS      2000   /* 写超时兜底 */

static pthread_mutex_t g_send_lock = PTHREAD_MUTEX_INITIALIZER;  /* 串行化 TCP 写 */
static pthread_mutex_t g_link_lock = PTHREAD_MUTEX_INITIALIZER;  /* 保护链路状态 */

static int g_link_online = 0;    /* 1=链路可用 */
static int g_fail_streak = 0;    /* 连续失败次数 */

/* 更新链路状态：连续失败达到阈值才判离线，避免单次抖动误判 */
static void unicast_mark_result(int ok)
{
    pthread_mutex_lock(&g_link_lock);
    if (ok != 0) {
        g_fail_streak = 0;
        g_link_online = 1;
    } else {
        g_fail_streak++;
        if (g_fail_streak >= HEARTBEAT_LOST_LIMIT) {
            g_link_online = 0;
        }
    }
    pthread_mutex_unlock(&g_link_lock);
}

/* 非阻塞 connect + poll 超时，成功返回 fd，失败返回负错误码 */
static int tcp_connect_timeout(const char *ip, uint16_t port, int timeout_ms)
{
    struct sockaddr_in addr;
    struct pollfd pfd;
    struct timeval tv;
    socklen_t elen = 0;
    int fd = -1;
    int flags = 0;
    int rc = 0;
    int so_err = 0;
    int nodelay = 1;

    fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        return IOT_ERR_IO;
    }

    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    if (inet_pton(AF_INET, ip, &addr.sin_addr) != 1) {
        close(fd);
        return IOT_ERR_GENERIC;
    }

    flags = fcntl(fd, F_GETFL, 0);
    if (flags < 0 || fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0) {
        close(fd);
        return IOT_ERR_IO;
    }

    rc = connect(fd, (struct sockaddr *)&addr, sizeof(addr));
    if (rc < 0 && errno != EINPROGRESS) {
        close(fd);
        return IOT_ERR_IO;
    }

    if (rc < 0) {
        pfd.fd = fd;
        pfd.events = POLLOUT;
        pfd.revents = 0;
        do {
            rc = poll(&pfd, 1, timeout_ms);
        } while (rc < 0 && errno == EINTR);

        if (rc == 0) {
            close(fd);
            return IOT_ERR_TIMEOUT;
        }
        if (rc < 0) {
            close(fd);
            return IOT_ERR_IO;
        }

        /* poll 可写不代表连上，必须用 SO_ERROR 复核 */
        elen = sizeof(so_err);
        if (getsockopt(fd, SOL_SOCKET, SO_ERROR, &so_err, &elen) < 0 || so_err != 0) {
            close(fd);
            return IOT_ERR_IO;
        }
    }

    /* 恢复阻塞模式，让 send 按 SO_SNDTIMEO 超时而不是返回 EAGAIN */
    if (fcntl(fd, F_SETFL, flags) < 0) {
        close(fd);
        return IOT_ERR_IO;
    }

    /* 小帧上报，禁用 Nagle 减少时延 */
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &nodelay, sizeof(nodelay));

    tv.tv_sec  = UNICAST_SEND_TIMEOUT_MS / 1000;
    tv.tv_usec = (UNICAST_SEND_TIMEOUT_MS % 1000) * 1000;
    setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));

    return fd;
}

/* 不加锁、不改状态的原始写：调用方负责持锁与状态更新 */
static int unicast_write_all(int fd, const void *buf, size_t len)
{
    const char *p = (const char *)buf;
    size_t remain = len;

    while (remain > 0u) {
        ssize_t n = send(fd, p, remain, MSG_NOSIGNAL);
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            return IOT_ERR_IO;
        }
        if (n == 0) {
            return IOT_ERR_IO;
        }
        p += n;
        remain -= (size_t)n;
    }
    return IOT_OK;
}

/* 取令牌后写：整帧数据按 TBF_MAX_FETCH_PER_OP 切片限速。
 * bucket 由调用方指定，实时上报用 g_outbound_bucket，
 * 断链补传用 g_retry_bucket，两者互不挤占 */
static int unicast_write_throttled(int fd, const void *buf, size_t len, tbf_t *bucket)
{
    const char *p = (const char *)buf;
    size_t remain = len;

    if (bucket == NULL) {
        return IOT_ERR_GENERIC;   /* 未初始化令牌桶，拒绝无限速发送 */
    }

    while (remain > 0u) {
        uint64_t want = (remain > TBF_MAX_FETCH_PER_OP) ? (uint64_t)TBF_MAX_FETCH_PER_OP
                                                        : (uint64_t)remain;
        int rc = tbf_fetch_token(bucket, want, -1);

        if (rc == IOT_ERR_SHUTDOWN) {
            return IOT_ERR_SHUTDOWN;
        }
        if (rc < 0) {
            continue;   /* 超时等异常：重新取令牌 */
        }
        if (unicast_write_all(fd, p, (size_t)want) != IOT_OK) {
            return IOT_ERR_IO;
        }
        p += want;
        remain -= (size_t)want;
    }
    return IOT_OK;
}

int unicast_sender_init(const char *ip, uint16_t port)
{
    int fd = -1;

    if (ip == NULL || port == 0u) {
        return IOT_ERR_GENERIC;
    }

    fd = tcp_connect_timeout(ip, port, UNICAST_CONNECT_TIMEOUT_MS);
    if (fd < 0) {
        unicast_mark_result(0);
        return fd;
    }

    unicast_mark_result(1);
    IOT_LOG(LOG_INFO, "platform link up: %s:%u", ip, port);
    return fd;
}

int unicast_sender_send_with_bucket(int fd, const void *buf, size_t len,
                                    tbf_t *bucket)
{
    int rc = IOT_OK;

    if (fd < 0 || buf == NULL || len == 0u) {
        return IOT_ERR_GENERIC;
    }

    /* 串行化：多个分发线程共用同一条 TCP 连接，
     * 不加锁的话两帧的字节会交错写进去，对端根本解不出来 */
    pthread_mutex_lock(&g_send_lock);
    rc = unicast_write_throttled(fd, buf, len, bucket);
    unicast_mark_result(rc == IOT_OK ? 1 : 0);
    pthread_mutex_unlock(&g_send_lock);

    return rc;
}

int unicast_sender_send(int fd, const void *buf, size_t len)
{
    /* 实时上报走出口桶 */
    return unicast_sender_send_with_bucket(fd, buf, len, g_outbound_bucket);
}

int unicast_sender_heartbeat(int fd)
{
    env_frame_t f;
    uint8_t wire[ENV_FRAME_WIRE_SIZE];
    int n = 0;
    int rc = IOT_OK;

    if (fd < 0) {
        return IOT_ERR_GENERIC;
    }

    /* 构造心跳帧：只填控制字段，测量字段留 0 */
    memset(&f, 0, sizeof(f));
    f.version = 1;
    f.frame_type = FRAME_HEART;
    f.timestamp_sec = (uint32_t)time(NULL);
    f.timestamp_ms = 0;

    frame_hton(&f);                                    /* 主机序 -> 网络序 */
    n = frame_encode(&f, wire, sizeof(wire));          /* 补 magic/version/CRC */
    if (n <= 0) {
        return IOT_ERR_PROTO;
    }

    /* 心跳是控制帧且体积极小，不走令牌桶，保证链路活性判定不被数据积压拖累 */
    pthread_mutex_lock(&g_send_lock);
    rc = unicast_write_all(fd, wire, (size_t)n);
    unicast_mark_result(rc == IOT_OK ? 1 : 0);
    pthread_mutex_unlock(&g_send_lock);

    return rc;
}

void unicast_sender_close(int fd)
{
    if (fd < 0) {
        return;
    }
    shutdown(fd, SHUT_RDWR);
    close(fd);

    pthread_mutex_lock(&g_link_lock);
    g_link_online = 0;
    g_fail_streak = 0;
    pthread_mutex_unlock(&g_link_lock);
}

int unicast_link_online(void)
{
    int online = 0;

    pthread_mutex_lock(&g_link_lock);
    online = g_link_online;
    pthread_mutex_unlock(&g_link_lock);

    return online;
}

uint32_t unicast_heartbeat_lost(void)
{
    uint32_t lost = 0;

    pthread_mutex_lock(&g_link_lock);
    lost = (uint32_t)g_fail_streak;
    pthread_mutex_unlock(&g_link_lock);

    return lost;
}
