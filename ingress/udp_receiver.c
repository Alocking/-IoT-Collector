//UDP接入 — epoll统一接入 + try-push + 协作退出
//  1) 被动端必须 bind;组播还须 IP_ADD_MEMBERSHIP(头文件 MCAST_* 宏)
//  2) 端口/地址网络字节序: htons(port)、htonl(INADDR_ANY)
//  3) 单播与组播共用一个 epoll 实例
//  4) 假错 EINTR/EAGAIN: continue;入队用 ring_push_try,避免阻塞接收
//  5) 统计: stats_on_ingress(0=ucast/1=mcast)、stats_on_decode_fail
//  6) 退出: g_running=0 → stop 关 socket/epoll → epoll_wait 返回 → join
#include "iot_collector.h"//引入项目总头文件

static pthread_t g_tid;//接收线程id
static int g_thread_created = 0;//线程是否已创建
static int g_epfd = -1;//epoll实例
static int g_ucast_fd = -1;//UDP单播套接字
static int g_mcast_fd = -1;//UDP组播套接字(-1表示未启用)
static ring_queue_t *g_push_q1 = NULL;//入队目标Q1(指针,避免与全局队列对象 g_q1 重名)
static volatile sig_atomic_t *g_run_flag = NULL;//运行标志指针

//初始化UDP单播接收socket,成功返回fd,失败-1
int udp_ucast_init(uint16_t port, struct sockaddr_in *out_addr)
{
    int fd = -1;//套接字
    int reuse = 1;//地址复用
    struct sockaddr_in addr;//绑定地址

    fd = socket(AF_INET, SOCK_DGRAM, 0);//UDP报式套接字
    if (fd < 0) {//创建失败
        perror("socket(udp-ucast)");
        return -1;
    }
    if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) < 0) {//端口复用
        perror("setsockopt(SO_REUSEADDR)");//非致命
    }
    memset(&addr, 0, sizeof(addr));//清零
    addr.sin_family = AF_INET;//IPv4
    addr.sin_addr.s_addr = htonl(INADDR_ANY);//监听所有网卡
    addr.sin_port = htons(port);//端口网络序
    if (bind(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {//必须bind
        perror("bind(udp-ucast)");
        close(fd);
        return -1;
    }
    if (out_addr != NULL) {//可选回填绑定地址
        *out_addr = addr;
    }
    return fd;//成功
}

//初始化UDP组播接收: bind + IP_ADD_MEMBERSHIP,成功返回fd
int udp_mcast_init(const char *group, uint16_t port, const char *ifname,
                   struct sockaddr_in *out_bind)
{
    int fd = -1;
    int reuse = 1;
    struct sockaddr_in addr;
    struct ip_mreqn mreq;//说明书§7.1 组播成员结构

    if (group == NULL || group[0] == '\0') {//必须有组地址
        return -1;
    }
    fd = socket(AF_INET, SOCK_DGRAM, 0);//组播必须基于UDP
    if (fd < 0) {
        perror("socket(udp-mcast)");
        return -1;
    }
    if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) < 0) {
        perror("setsockopt(SO_REUSEADDR-mcast)");
    }
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);//绑到通配地址,由内核分发组播
    addr.sin_port = htons(port);//组播端口
    if (bind(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {//组播接收也要bind
        perror("bind(udp-mcast)");
        close(fd);
        return -1;
    }
    memset(&mreq, 0, sizeof(mreq));//清零成员描述
    if (inet_pton(AF_INET, group, &mreq.imr_multiaddr) != 1) {//组地址转换
        fprintf(stderr, "inet_pton 无效组播地址 : %s\n", group);
        close(fd);
        return -1;
    }
    mreq.imr_address.s_addr = htonl(INADDR_ANY);//默认路由选网卡
    mreq.imr_ifindex = 0;//0=由系统选择;指定网卡时用 if_nametoindex
    if (ifname != NULL && ifname[0] != '\0') {//指定了网卡名
        mreq.imr_ifindex = (int)if_nametoindex(ifname);//如 eth0
        if (mreq.imr_ifindex == 0) {
            fprintf(stderr, "if_nametoindex 失败 : %s\n", ifname);
            close(fd);
            return -1;
        }
    }
    if (setsockopt(fd, IPPROTO_IP, IP_ADD_MEMBERSHIP, &mreq, sizeof(mreq)) < 0) {//加入组播组
        perror("setsockopt(IP_ADD_MEMBERSHIP)");
        close(fd);
        return -1;
    }
    if (out_bind != NULL) {//回填bind地址
        *out_bind = addr;
    }
    return fd;//成功
}

//epoll 注册辅助
static int epoll_add_fd(int epfd, int fd)
{
    struct epoll_event ev;//事件
    if (epfd < 0 || fd < 0) {//非法
        return -1;
    }
    memset(&ev, 0, sizeof(ev));
    ev.events = EPOLLIN;//可读
    ev.data.fd = fd;//携带fd便于回调识别单播/组播
    if (epoll_ctl(epfd, EPOLL_CTL_ADD, fd, &ev) < 0) {//加入epoll
        perror("epoll_ctl(ADD)");
        return -1;
    }
    return 0;
}

//收包线程入口: epoll_wait → recvfrom → decode_wire → ring_push_try
static void *udp_recv_thread(void *arg)
{
    struct epoll_event events[INGRESS_EPOLL_MAX_EV];//就绪事件数组
    uint8_t buf[512];//收包缓冲区
    (void)arg;//未使用参数

    while (g_run_flag != NULL && (*g_run_flag) != 0) {//协作退出
        int n = 0;//就绪事件数
        int i = 0;//事件下标
        if (g_epfd < 0) {//epoll已关
            break;
        }
        //超时200ms:即使无包也能回到循环头查看退出标志
        n = epoll_wait(g_epfd, events, INGRESS_EPOLL_MAX_EV, 200);
        if (n < 0) {//等待失败
            if (errno == EINTR || errno == EAGAIN) {//假错
                continue;
            }
            perror("epoll_wait(udp)");
            continue;//长跑服务继续
        }
        for (i = 0; i < n; i++) {//处理本轮就绪fd
            int fd = events[i].data.fd;//就绪套接字
            struct sockaddr_in peer;//对端
            socklen_t slen = sizeof(peer);
            ssize_t rn = 0;//收包字节数
            env_frame_t frame;//主机序帧
            int proto = 0;//统计协议号: 0=ucast 1=mcast
            int rc = 0;//入队结果

            if (events[i].events & (EPOLLERR | EPOLLHUP)) {//异常
                continue;//本轮跳过,由 stop/重开处理
            }
            if (fd == g_mcast_fd) {//组播fd
                proto = 1;
            } else {//单播或未知
                proto = 0;
            }
            memset(&peer, 0, sizeof(peer));
            rn = recvfrom(fd, buf, sizeof(buf), 0,//收一包
                          (struct sockaddr *)&peer, &slen);
            if (rn < 0) {//收包失败
                if (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK) {//假错
                    continue;
                }
                perror("recvfrom(udp)");
                continue;
            }
            if (decode_wire(buf, (size_t)rn, &frame) != 0) {//magic/CRC失败
                stats_on_decode_fail(1);//协议错误统计
                continue;//丢弃
            }
            //说明书§4.2: 接收线程 try-push,队列满不阻塞,避免内核缓冲溢出
            rc = ring_push_try(g_push_q1, &frame);
            if (rc == IOT_OK) {//入队成功才计接入帧
                stats_on_ingress(proto, 1);//0单播/1组播
            } else {
                stats_on_drop(1);//队列满丢弃(告警挤占策略由 ring 层处理)
            }
        }
    }
    return NULL;//线程返回
}

//启动主接口: 按配置启动单播(+可选组播) epoll接收线程
int udp_receiver_start(const udp_ingress_cfg_t *cfg, volatile sig_atomic_t *running)
{
    struct sockaddr_in uaddr;//单播绑定地址
    struct sockaddr_in maddr;//组播绑定地址
    int ret = 0;//pthread返回值

    if (cfg == NULL || cfg->q1 == NULL || running == NULL) {//参数校验
        return IOT_ERR_GENERIC;
    }
    if (cfg->ucast_port == 0 && (cfg->mcast_group == NULL || cfg->mcast_port == 0)) {
        return IOT_ERR_GENERIC;//单播/组播至少启用一个
    }

    g_push_q1 = cfg->q1;//记录队列
    g_run_flag = running;//记录运行标志
    g_ucast_fd = -1;
    g_mcast_fd = -1;

    g_epfd = epoll_create1(0);//创建epoll实例
    if (g_epfd < 0) {
        perror("epoll_create1(udp)");
        return IOT_ERR_IO;
    }

    if (cfg->ucast_port != 0) {//启用单播
        g_ucast_fd = udp_ucast_init(cfg->ucast_port, &uaddr);
        if (g_ucast_fd < 0) {//失败清理
            close(g_epfd);
            g_epfd = -1;
            return IOT_ERR_IO;
        }
        if (epoll_add_fd(g_epfd, g_ucast_fd) != 0) {//注册可读事件
            close(g_ucast_fd);
            close(g_epfd);
            g_ucast_fd = g_epfd = -1;
            return IOT_ERR_IO;
        }
    }

    if (cfg->mcast_group != NULL && cfg->mcast_port != 0) {//启用组播(FR-01)
        g_mcast_fd = udp_mcast_init(cfg->mcast_group, cfg->mcast_port,
                                    cfg->mcast_ifname, &maddr);
        if (g_mcast_fd < 0) {//组播失败则退回仅单播(若单播已起来)
            if (g_ucast_fd < 0) {//单播也没有
                close(g_epfd);
                g_epfd = -1;
                return IOT_ERR_IO;
            }
            //单播可用:记日志后继续,不整进程失败
            IOT_LOG(LOG_WARNING, "mcast init failed, ucast only");
        } else if (epoll_add_fd(g_epfd, g_mcast_fd) != 0) {//组播加入epoll失败
            close(g_mcast_fd);
            g_mcast_fd = -1;
        }
    }

    ret = pthread_create(&g_tid, NULL, udp_recv_thread, NULL);//创建收包线程
    if (ret != 0) {//创建失败
        fprintf(stderr, "pthread_create(udp) : %s\n", strerror(ret));
        if (g_ucast_fd >= 0) close(g_ucast_fd);
        if (g_mcast_fd >= 0) close(g_mcast_fd);
        close(g_epfd);
        g_ucast_fd = g_mcast_fd = g_epfd = -1;
        return IOT_ERR_GENERIC;
    }
    g_thread_created = 1;//标记线程已创建
    return IOT_OK;//成功
}

//停止接口: 关闭socket与epoll,让 epoll_wait/recvfrom 尽快返回
void udp_receiver_stop(void)
{
    if (g_ucast_fd >= 0) {//关闭单播
        close(g_ucast_fd);
        g_ucast_fd = -1;
    }
    if (g_mcast_fd >= 0) {//关闭组播
        close(g_mcast_fd);
        g_mcast_fd = -1;
    }
    if (g_epfd >= 0) {//关闭epoll
        close(g_epfd);
        g_epfd = -1;
    }
}

//等待线程结束
int udp_receiver_join(void)
{
    if (g_thread_created == 0) {//线程未创建
        return IOT_OK;
    }
    pthread_join(g_tid, NULL);//回收线程
    g_thread_created = 0;
    return IOT_OK;
}
