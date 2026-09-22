//运行统计 — 五要点
//  1) 多线程共享计数必须加锁，否则竞态丢数
//  2) 对外接口 stats_on_*：ingress/clean/drop/egress/throttled 分类累加
//  3) 水位：stats_bind_queues 后 snapshot 时 ring_count 读 Q1/Q2
//  4) 运行态字段（线程数/链路）由主控每秒 stats_update_runtime 写入
//  5) stats_report：syslog + stdout 双输出，便于演示对照 README
#include "iot_collector.h"//引入项目总头文件

static iot_stats_t g_stats;//全局统计结构
static ring_queue_t *g_stat_q1 = NULL;//绑定的 Q1（可空；避免与全局队列对象重名）
static ring_queue_t *g_stat_q2 = NULL;//绑定的 Q2（可空）
static pthread_mutex_t stats_lock = PTHREAD_MUTEX_INITIALIZER;//保护 g_stats
static pthread_t stats_tid;//统计线程 id
static volatile int stats_thread_run = 0;//统计线程退出标志
static uint64_t g_start_ms = 0;//启动时刻，用于算 uptime

//取当前毫秒时间戳（单调时钟，不受系统时间调整影响）
static uint64_t now_ms(void)
{
    struct timespec ts;//时间戳
    clock_gettime(CLOCK_MONOTONIC, &ts);//单调时钟
    return (uint64_t)ts.tv_sec * 1000u + (uint64_t)ts.tv_nsec / 1000000u;//换算毫秒
}

//统计线程入口：每秒打印一次（STATS_INTERVAL_SEC）
static void *stats_thread_fn(void *arg)
{
    (void)arg;//未使用
    while (stats_thread_run) {//运行中
        sleep(STATS_INTERVAL_SEC);//休眠 1 秒
        stats_report();//输出报表
    }
    return NULL;//线程结束
}

//初始化统计模块：清零 + 记启动时间 + 拉起统计线程
void stats_init(void)
{
    memset(&g_stats, 0, sizeof(g_stats));//全部清零
    g_start_ms = now_ms();//记录启动时刻
    stats_thread_run = 1;//允许线程运行
    pthread_create(&stats_tid, NULL, stats_thread_fn, NULL);//创建统计线程
}

//绑定 Q1/Q2，使快照能带出队列水位
void stats_bind_queues(ring_queue_t *q1, ring_queue_t *q2)
{
    pthread_mutex_lock(&stats_lock);//加锁
    g_stat_q1 = q1;//绑定 Q1
    g_stat_q2 = q2;//绑定 Q2
    pthread_mutex_unlock(&stats_lock);//解锁
}

//记录接入层收帧：proto 0=ucast 1=mcast 2=tcp 3=sim
void stats_on_ingress(int proto, uint32_t n)
{
    pthread_mutex_lock(&stats_lock);//临界区
    g_stats.ingress_total += n;//总量
    switch (proto) {//按通道分列
        case 0: g_stats.udp_ucast_frames += n; break;//单播
        case 1: g_stats.udp_mcast_frames += n; break;//组播
        case 2: g_stats.tcp_frames += n; break;//TCP
        case 3: g_stats.sim_frames += n; break;//内置模拟
    }
    pthread_mutex_unlock(&stats_lock);//离开临界区
}

//记录解码失败（magic/CRC）
void stats_on_decode_fail(uint32_t n)
{
    pthread_mutex_lock(&stats_lock);
    g_stats.decode_fail += n;
    pthread_mutex_unlock(&stats_lock);
}

//记录清洗结果：kind 0=pass 1=alarm 2=range_drop 3=dup_drop 4=reorder_drop
void stats_on_clean(int kind, uint32_t n)
{
    pthread_mutex_lock(&stats_lock);
    switch (kind) {
        case 0: g_stats.cleaned_frames += n; break;//通过
        case 1: g_stats.anomaly_frames += n; break;//告警/突变
        case 2: g_stats.range_drop += n; break;//量程丢弃
        case 3: g_stats.dup_drop += n; break;//重复丢弃
        case 4: g_stats.reorder_drop += n; break;//乱序丢弃
    }
    pthread_mutex_unlock(&stats_lock);
}

//记录丢帧（队列满等）
void stats_on_drop(uint32_t n)
{
    pthread_mutex_lock(&stats_lock);
    g_stats.dropped_total += n;
    pthread_mutex_unlock(&stats_lock);
}

//记录出口发帧：channel 0=mcast 1=unicast
void stats_on_egress(int channel, uint32_t n)
{
    pthread_mutex_lock(&stats_lock);
    if (channel == 0) {
        g_stats.mcast_ok += n;//组播成功
    } else {
        g_stats.unicast_ok += n;//单播成功
    }
    pthread_mutex_unlock(&stats_lock);
}

//记录限速字节数
void stats_on_throttled(uint64_t bytes)
{
    pthread_mutex_lock(&stats_lock);
    g_stats.throttled_bytes += bytes;
    pthread_mutex_unlock(&stats_lock);
}

//获取统计快照（含队列水位）
void stats_snapshot(iot_stats_t *out)
{
    if (out == NULL) return;//判空
    pthread_mutex_lock(&stats_lock);
    if (g_stat_q1 != NULL) {//刷新 Q1 水位
        g_stats.q1_count = ring_count(g_stat_q1);//当前元素数
        g_stats.q1_cap = g_stat_q1->capacity;//容量
        g_stats.dropped_total = g_stat_q1->dropped + (g_stat_q2 ? g_stat_q2->dropped : 0);//累计丢帧
    }
    if (g_stat_q2 != NULL) {//刷新 Q2 水位
        g_stats.q2_count = ring_count(g_stat_q2);
        g_stats.q2_cap = g_stat_q2->capacity;
    }
    g_stats.cache_pending = cache_pending_frames();//断链缓存积压
    memcpy(out, &g_stats, sizeof(iot_stats_t));//拷出快照
    pthread_mutex_unlock(&stats_lock);
}

//刷新运行态字段：线程数 / 忙线程 / 链路状态（由主控每秒写入）
void stats_update_runtime(uint32_t parser_workers, uint32_t parser_busy,
                          uint32_t dispatcher_workers, uint32_t dispatcher_busy,
                          int platform_link_up, uint32_t heartbeat_lost)
{
    pthread_mutex_lock(&stats_lock);
    g_stats.parser_workers = parser_workers;
    g_stats.parser_busy = parser_busy;
    g_stats.dispatcher_workers = dispatcher_workers;
    g_stats.dispatcher_busy = dispatcher_busy;
    g_stats.platform_link_up = platform_link_up;//1=在线
    g_stats.heartbeat_lost = heartbeat_lost;//连续失败次数
    pthread_mutex_unlock(&stats_lock);
}

//每秒输出统计：syslog(DEBUG) + stdout，格式对齐 README 示例
void stats_report(void)
{
    iot_stats_t snap;//快照
    double uptime = 0.0;//运行秒数
    double rate = 0.0;//接入速率

    stats_snapshot(&snap);//取快照
    uptime = (double)(now_ms() - g_start_ms) / 1000.0;//秒
    if (uptime > 0.01) {
        rate = (double)snap.ingress_total / uptime;//帧/秒
    }

    /* 格式与 README「运行中每秒统计示例」保持一致 */
    IOT_LOG(LOG_INFO,
        "uptime=%.1fs ingress=%llu rate=%.1f/s decode_fail=%llu "
        "Q1=%u/%u Q2=%u/%u cleaned=%llu anomaly=%llu "
        "mcast_ok=%llu unicast_ok=%llu cache=%llu link=%s",
        uptime,
        (unsigned long long)snap.ingress_total, rate,
        (unsigned long long)snap.decode_fail,
        snap.q1_count, snap.q1_cap,
        snap.q2_count, snap.q2_cap,
        (unsigned long long)snap.cleaned_frames,
        (unsigned long long)snap.anomaly_frames,
        (unsigned long long)snap.mcast_ok,
        (unsigned long long)snap.unicast_ok,
        (unsigned long long)snap.cache_pending,
        snap.platform_link_up ? "ONLINE" : "SINK");

    /* 控制台同步打印，便于 make 运行后肉眼验收 */
    printf("[IoT-Collector] uptime=%.1fs\n", uptime);
    printf("ingress : udp_ucast=%llu udp_mcast=%llu tcp=%llu sim=%llu total=%llu rate=%.1f/s decode_fail=%llu\n",
           (unsigned long long)snap.udp_ucast_frames,
           (unsigned long long)snap.udp_mcast_frames,
           (unsigned long long)snap.tcp_frames,
           (unsigned long long)snap.sim_frames,
           (unsigned long long)snap.ingress_total, rate,
           (unsigned long long)snap.decode_fail);
    printf("queue   : Q1=%u/%u(drop=%llu) Q2=%u/%u cleaned=%llu anomaly=%llu range_drop=%llu\n",
           snap.q1_count, snap.q1_cap,
           (unsigned long long)snap.dropped_total,
           snap.q2_count, snap.q2_cap,
           (unsigned long long)snap.cleaned_frames,
           (unsigned long long)snap.anomaly_frames,
           (unsigned long long)snap.range_drop);
    printf("egress  : mcast_ok=%llu unicast_ok=%llu cache=%llu throttled_bytes=%llu\n",
           (unsigned long long)snap.mcast_ok,
           (unsigned long long)snap.unicast_ok,
           (unsigned long long)snap.cache_pending,
           (unsigned long long)snap.throttled_bytes);
    printf("link    : platform=%s heartbeat_lost=%u\n",
           snap.platform_link_up ? "ONLINE" : "SINK",
           snap.heartbeat_lost);
    fflush(stdout);//立即刷出
}
