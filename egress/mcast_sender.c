//组播分发出口 — 四要点
//  1) IP_MULTICAST_IF：指定出口网卡，多网卡不设会走默认路由
//  2) IP_MULTICAST_TTL：默认 1 仅本网段，跨网段必须放大（本项目 MCAST_TTL=16）
//  3) IP_MULTICAST_LOOP=0：关闭自发自收，否则本机 ingress 会把出口帧再采回来形成回环放大
//  4) 组播一对多、按 FR-05 不限速，直接 sendto，不走令牌桶
#include "iot_collector.h"//引入项目总头文件

static struct sockaddr_in g_mcast_dest;   /* 组播目的地址（组播组:端口） */
static int                g_dest_ready = 0;

int mcast_sender_init(const char *group, uint16_t port, const char *ifname,
                      uint8_t ttl)
{
    struct ip_mreqn mif;
    uint8_t ttl_val = ttl;
    uint8_t loop_val = 0;//关闭组播回环：本进程不接收自己发出的组播报文
    int fd = -1;

    if (group == NULL || port == 0u) {
        return IOT_ERR_GENERIC;
    }

    memset(&g_mcast_dest, 0, sizeof(g_mcast_dest));
    g_mcast_dest.sin_family = AF_INET;
    g_mcast_dest.sin_port = htons(port);
    if (inet_pton(AF_INET, group, &g_mcast_dest.sin_addr) != 1) {
        return IOT_ERR_GENERIC;
    }
    g_dest_ready = 0;

    fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (fd < 0) {
        return IOT_ERR_IO;
    }

    /* 指定组播出口网卡；ifname 为 NULL 时交给系统默认路由 */
    if (ifname != NULL) {
        memset(&mif, 0, sizeof(mif));
        mif.imr_ifindex = (int)if_nametoindex(ifname);
        if (mif.imr_ifindex == 0) {
            close(fd);
            return IOT_ERR_GENERIC;
        }
        if (setsockopt(fd, IPPROTO_IP, IP_MULTICAST_IF, &mif, sizeof(mif)) < 0) {
            close(fd);
            return IOT_ERR_IO;
        }
    }

    /* TTL：0 视为未指定，取默认值（跨网段必须显式放大） */
    if (ttl_val == 0u) {
        ttl_val = (uint8_t)MCAST_TTL;
    }
    if (setsockopt(fd, IPPROTO_IP, IP_MULTICAST_TTL, &ttl_val,
                   sizeof(ttl_val)) < 0) {
        close(fd);
        return IOT_ERR_IO;
    }

    /* 关闭自发自收：防止 ingress 把 egress 组播报文再采回 Q1 */
    if (setsockopt(fd, IPPROTO_IP, IP_MULTICAST_LOOP, &loop_val,
                   sizeof(loop_val)) < 0) {
        /* 某些平台不支持则忽略，靠收发组地址分离兜底 */
    }

    g_dest_ready = 1;
    return fd;    /* 成功返回 fd，供 mcast_sender_send 使用；失败返回负错误码 */
}

int mcast_sender_send(int fd, const void *buf, size_t len)
{
    ssize_t n = 0;

    if (fd < 0 || buf == NULL || len == 0u) {
        return IOT_ERR_GENERIC;
    }
    if (g_dest_ready == 0) {
        return IOT_ERR_GENERIC;
    }

    do {
        n = sendto(fd, buf, len, 0,
                   (const struct sockaddr *)&g_mcast_dest,
                   sizeof(g_mcast_dest));
    } while (n < 0 && errno == EINTR);

    if (n < 0) {
        return IOT_ERR_IO;
    }
    /* UDP 是整包语义，短写视为失败，避免发出残缺数据报 */
    if ((size_t)n != len) {
        return IOT_ERR_IO;
    }
    return IOT_OK;
}
