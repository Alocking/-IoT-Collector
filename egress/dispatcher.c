//分发与限速层 egress — 五要点
//  1) worker 线程：ring_pop_batch(Q2) → 编码 → 组播(不限速)+单播(令牌桶限速)
//  2) 监控线程：30s 心跳；离线每 5s 重连，成功后 cache_replay 补传
//  3) 时钟线程：TBF_TICK_MS 补令牌，活到所有 worker 退出之后，否则冲刷饿死
//  4) 单播失败且配置了 cache_path → cache_writer_append，恢复后补传
//  5) 退出：g_running=0 → ring_wake_all(Q2) → join worker → 停时钟/监控
#include "iot_collector.h"//引入项目总头文件

#define RECONNECT_INTERVAL_SEC   5      /* 断链重连尝试间隔 */
#define MONITOR_TICK_SEC         1      /* 监控线程轮询粒度 */

/* ---- 对外配置（字符串拷贝到本地，不依赖调用方生命周期） ---- */
static dispatcher_cfg_t g_cfg;
static char g_mcast_group_buf[64];
static char g_mcast_ifname_buf[IF_NAMESIZE];
static char g_platform_ip_buf[64];
static char g_cache_path_buf[256];

/* ---- 线程与出口句柄 ---- */
static pthread_t       g_tids[POOL_MAX_WORKERS];
static pthread_t       g_mon_tid;
static pthread_t       g_tick_tid;
static int             g_mon_started = 0;
static int             g_tick_started = 0;
static uint32_t        g_worker_num = 0;
static int             g_started = 0;

static int             g_mcast_fd = -1;
static int             g_unicast_fd = -1;
static int             g_own_bucket = 0;    /* 令牌桶是否由本层创建（决定是否负责销毁） */

/* 注意：不能叫 g_running —— 头文件里已声明同名全局运行标志 */
static volatile sig_atomic_t *g_run_flag = NULL;
static volatile int            g_stop = 0;

/* 时钟线程单独一个停止标志。
 * 为什么不能跟着 g_stop 一起停：优雅退出时我们要先把 Q2 里积压的帧冲刷完，
 * 而冲刷需要令牌。如果时钟线程先停了，还卡在等令牌上的分发线程就永远醒不来，
 * dispatcher_join() 会直接死等。所以时钟线程要活到「所有 worker 都退出之后」 */
static volatile int            g_tick_stop = 0;

/* 保护 g_unicast_fd 的替换与使用：重连时 close 旧 fd 不能和正在发送的线程撞车 */
static pthread_mutex_t g_unicast_lock = PTHREAD_MUTEX_INITIALIZER;

/* 拷贝可选字符串；src 为 NULL 时置空并返回 0 */
static int copy_opt_str(char *dst, size_t cap, const char *src, int *is_set)
{
    size_t len = 0;

    if (src == NULL) {
        dst[0] = '\0';
        *is_set = 0;
        return 0;
    }
    len = strlen(src);
    if (len >= cap) {
        return -1;
    }
    memcpy(dst, src, len + 1);
    *is_set = 1;
    return 0;
}

/* 单帧分发：编码后走组播 + 单播两条出口 */
static void dispatch_one(const env_frame_t *frame, uint8_t *wire)
{
    env_frame_t out;
    int len = 0;

    out = *frame;
    frame_hton(&out);                                  /* 主机序 -> 网络序 */
    len = frame_encode(&out, wire, ENV_FRAME_WIRE_SIZE);
    if (len <= 0) {
        stats_on_drop(1);
        return;
    }

    /* 组播分发：一对多，按 FR-05 不限速 */
    if (g_mcast_fd >= 0) {
        if (mcast_sender_send(g_mcast_fd, wire, (size_t)len) == IOT_OK) {
            stats_on_egress(0, 1);
        }
    }

    /* 单播上报：令牌桶限速；链路不可用则落断链缓存，等恢复后补传 */
    if (g_cfg.platform_ip != NULL) {
        int rc = IOT_ERR_IO;

        pthread_mutex_lock(&g_unicast_lock);
        if (g_unicast_fd >= 0 && unicast_link_online() != 0) {
            rc = unicast_sender_send(g_unicast_fd, wire, (size_t)len);
        }
        pthread_mutex_unlock(&g_unicast_lock);

        if (rc == IOT_OK) {
            stats_on_egress(1, 1);
        } else if (g_cfg.cache_path != NULL) {
            cache_writer_append(frame);   /* 缓存主机序帧，补传时重新编码 */
        } else {
            stats_on_drop(1);
        }
    }
}

static void *dispatch_worker(void *arg)
{
    env_frame_t batch[RINGQ_BATCH_MAX];
    uint8_t wire[ENV_FRAME_WIRE_SIZE];

    (void)arg;

    for (;;) {
        /* 批量出队：空则条件变量等待；g_running=0 时返回 0 */
        uint32_t n = ring_pop_batch(g_cfg.q2, batch, RINGQ_BATCH_MAX);
        uint32_t i = 0;

        if (n == 0u) {
            if (g_stop != 0 || g_run_flag == NULL || *g_run_flag == 0) {
                break;
            }
            continue;
        }
        for (i = 0; i < n; i++) {
            dispatch_one(&batch[i], wire);
        }
    }
    return NULL;
}

/* 令牌桶时钟线程：每 TBF_TICK_MS 补充一次令牌并唤醒等待者。
 * 没有它，桶内令牌耗尽后取令牌的线程会永久阻塞。
 * 补令牌用的是「真实流逝毫秒」而非固定 TBF_TICK_MS：
 * nanosleep 会漂移、也会被信号打断，若按固定值补，长期速率会偏离 CPS。 */
static void *dispatch_ticker(void *arg)
{
    struct timespec period;
    struct timespec last;
    struct timespec now;

    (void)arg;

    period.tv_sec  = 0;
    period.tv_nsec = (long)TBF_TICK_MS * 1000000L;
    clock_gettime(CLOCK_MONOTONIC, &last);

    while (g_tick_stop == 0 && g_run_flag != NULL && *g_run_flag != 0) {
        int64_t  diff_ns = 0;
        uint64_t elapsed_ms = 0;

        clock_nanosleep(CLOCK_MONOTONIC, 0, &period, NULL);

        clock_gettime(CLOCK_MONOTONIC, &now);
        diff_ns = (int64_t)(now.tv_sec - last.tv_sec) * 1000000000LL
                + (int64_t)(now.tv_nsec - last.tv_nsec);
        last = now;

        if (diff_ns <= 0) {
            continue;                       /* 被打断的极短周期，不补令牌 */
        }
        elapsed_ms = (uint64_t)diff_ns / 1000000ULL;
        if (elapsed_ms == 0u) {
            continue;
        }

        if (g_outbound_bucket != NULL) {
            tbf_tick(g_outbound_bucket, elapsed_ms);
        }
        if (g_retry_bucket != NULL) {
            tbf_tick(g_retry_bucket, elapsed_ms);
        }
    }
    return NULL;
}

/* 监控线程：心跳保活 + 断链重连 + 恢复后补传 */
static void *dispatch_monitor(void *arg)
{
    time_t last_hb = time(NULL);
    time_t last_retry = 0;
    time_t last_replay = 0;

    (void)arg;

    while (g_stop == 0 && g_run_flag != NULL && *g_run_flag != 0) {
        sleep(MONITOR_TICK_SEC);

        if (g_cfg.platform_ip == NULL) {
            continue;
        }

        /* 心跳：在线时每 HEARTBEAT_INTERVAL_SEC 一次 */
        if (unicast_link_online() != 0 &&
            difftime(time(NULL), last_hb) >= (double)HEARTBEAT_INTERVAL_SEC) {
            pthread_mutex_lock(&g_unicast_lock);
            if (g_unicast_fd >= 0) {
                unicast_sender_heartbeat(g_unicast_fd);
            }
            pthread_mutex_unlock(&g_unicast_lock);
            last_hb = time(NULL);
        }

        /* 断链重连：离线时按间隔重试。
         * 这里只负责把连接建起来，补传统一交给下面那段处理 */
        if (unicast_link_online() == 0 &&
            difftime(time(NULL), last_retry) >= (double)RECONNECT_INTERVAL_SEC) {
            int fd = unicast_sender_init(g_cfg.platform_ip, g_cfg.platform_port);

            pthread_mutex_lock(&g_unicast_lock);
            if (g_unicast_fd >= 0) {
                unicast_sender_close(g_unicast_fd);
            }
            g_unicast_fd = (fd >= 0) ? fd : -1;
            pthread_mutex_unlock(&g_unicast_lock);

            last_retry = time(NULL);
        }

        /* 补传：链路在线 + 缓存有积压就按序补出去。
         * 两种情况都会走到这里：
         *   1) 刚刚断链重连成功
         *   2) 进程启动时链路就是通的，但上次退出时缓存里还剩着没发完的数据
         *      （只做情况 1 的话，这批数据要等到下一次断链才会被补传）
         * 用独立的 last_replay 节流，不和重连的计时混在一起：
         * 补传中途失败会停在读游标上，下一轮再接着补，不会丢也不会重 */
        if (g_stop == 0 && g_cfg.cache_path != NULL &&
            unicast_link_online() != 0 &&
            cache_pending_frames() > 0u &&
            difftime(time(NULL), last_replay) >= (double)RECONNECT_INTERVAL_SEC) {
            int fd = -1;

            pthread_mutex_lock(&g_unicast_lock);
            fd = g_unicast_fd;
            pthread_mutex_unlock(&g_unicast_lock);

            if (fd >= 0) {
                IOT_LOG(LOG_INFO, "replaying cached frames: pending=%llu",
                        (unsigned long long)cache_pending_frames());
                cache_replay_start(fd);
                IOT_LOG(LOG_INFO, "replay done: pending=%llu",
                        (unsigned long long)cache_pending_frames());
            }
            last_replay = time(NULL);
        }
    }
    return NULL;
}

int dispatcher_start(const dispatcher_cfg_t *cfg, volatile sig_atomic_t *running)
{
    int has_mcast = 0;
    int has_ifname = 0;
    int has_platform = 0;
    int has_cache = 0;
    uint32_t i = 0;

    if (cfg == NULL || cfg->q2 == NULL || running == NULL) {
        return IOT_ERR_GENERIC;
    }
    if (g_started != 0) {
        return IOT_ERR_GENERIC;    /* 禁止重复启动 */
    }

    memset(&g_cfg, 0, sizeof(g_cfg));
    g_cfg.q2 = cfg->q2;
    g_cfg.worker_num = (cfg->worker_num == 0u) ? POOL_MIN_WORKERS : cfg->worker_num;
    if (g_cfg.worker_num > POOL_MAX_WORKERS) {
        g_cfg.worker_num = POOL_MAX_WORKERS;
    }
    g_cfg.mcast_port = cfg->mcast_port;
    g_cfg.mcast_ttl = cfg->mcast_ttl;
    g_cfg.platform_port = cfg->platform_port;

    if (copy_opt_str(g_mcast_group_buf, sizeof(g_mcast_group_buf),
                     cfg->mcast_group, &has_mcast) != 0 ||
        copy_opt_str(g_mcast_ifname_buf, sizeof(g_mcast_ifname_buf),
                     cfg->mcast_ifname, &has_ifname) != 0 ||
        copy_opt_str(g_platform_ip_buf, sizeof(g_platform_ip_buf),
                     cfg->platform_ip, &has_platform) != 0 ||
        copy_opt_str(g_cache_path_buf, sizeof(g_cache_path_buf),
                     cfg->cache_path, &has_cache) != 0) {
        return IOT_ERR_GENERIC;
    }

    g_cfg.mcast_group  = (has_mcast != 0)    ? g_mcast_group_buf  : NULL;
    g_cfg.mcast_ifname = (has_ifname != 0)   ? g_mcast_ifname_buf : NULL;
    g_cfg.platform_ip  = (has_platform != 0) ? g_platform_ip_buf  : NULL;
    g_cfg.cache_path   = (has_cache != 0)    ? g_cache_path_buf   : NULL;

    g_run_flag = running;
    g_stop = 0;
    g_tick_stop = 0;
    g_mcast_fd = -1;
    g_unicast_fd = -1;

    /* 令牌桶：外部已初始化则复用，否则按默认参数（512KB/s，1MB 突发）自建 */
    if (g_outbound_bucket == NULL) {
        if (tbf_init(&g_outbound_bucket, TBF_DEFAULT_CPS, TBF_DEFAULT_BURST) != IOT_OK) {
            return IOT_ERR_NOMEM;
        }
        g_own_bucket = 1;
    }

    /* 令牌桶时钟线程：先于分发线程启动，保证取令牌不会饿死 */
    if (pthread_create(&g_tick_tid, NULL, dispatch_ticker, NULL) != 0) {
        IOT_LOG(LOG_ERR, "pthread_create(ticker) failed");
        g_tick_started = 0;
    } else {
        g_tick_started = 1;
    }

    /* 组播出口：初始化失败不致命，单播通道仍可上报 */
    if (g_cfg.mcast_group != NULL) {
        g_mcast_fd = mcast_sender_init(g_cfg.mcast_group, g_cfg.mcast_port,
                                       g_cfg.mcast_ifname, g_cfg.mcast_ttl);
        if (g_mcast_fd < 0) {
            IOT_LOG(LOG_ERR, "mcast_sender_init failed: %d", g_mcast_fd);
        }
    }

    /* 断链缓存 */
    if (g_cfg.cache_path != NULL) {
        cache_writer_open(g_cfg.cache_path);
    }

    /* 单播长连接：首次连不上转离线模式，由监控线程重连 */
    if (g_cfg.platform_ip != NULL) {
        g_unicast_fd = unicast_sender_init(g_cfg.platform_ip, g_cfg.platform_port);
    }

    /* 分发线程 */
    for (i = 0; i < g_cfg.worker_num; i++) {
        if (pthread_create(&g_tids[i], NULL, dispatch_worker, NULL) != 0) {
            IOT_LOG(LOG_ERR, "pthread_create(dispatcher) failed at %u", i);
            g_cfg.worker_num = i;
            break;
        }
    }
    if (g_cfg.worker_num == 0u) {
        if (g_unicast_fd >= 0) {
            unicast_sender_close(g_unicast_fd);
            g_unicast_fd = -1;
        }
        if (g_mcast_fd >= 0) {
            close(g_mcast_fd);
            g_mcast_fd = -1;
        }
        return IOT_ERR_GENERIC;
    }
    g_worker_num = g_cfg.worker_num;

    /* 监控线程：心跳与重连只对单播链路有意义 */
    if (g_cfg.platform_ip != NULL) {
        if (pthread_create(&g_mon_tid, NULL, dispatch_monitor, NULL) != 0) {
            IOT_LOG(LOG_ERR, "pthread_create(monitor) failed");
            g_mon_started = 0;
        } else {
            g_mon_started = 1;
        }
    }

    g_started = 1;
    IOT_LOG(LOG_INFO, "dispatcher started: workers=%u mcast=%s unicast=%s",
            g_worker_num,
            (g_cfg.mcast_group != NULL) ? g_cfg.mcast_group : "off",
            (g_cfg.platform_ip != NULL) ? g_cfg.platform_ip : "off");
    return IOT_OK;
}

/* 当前分发线程数。主控每秒出报表时读一次，
 * 直接返回启动时确定的 worker 数即可（本层不做动态扩缩容） */
uint32_t dispatcher_worker_count(void)
{
    return g_worker_num;
}

void dispatcher_stop(void)
{
    if (g_started == 0) {
        return;
    }
    g_stop = 1;

    /* 只唤醒 Q2，不关令牌桶。
     * 分发线程的退出条件是「队列取空」，所以它会把积压的帧继续发完再退；
     * 令牌桶的关闭推迟到 dispatcher_join()，否则最后一批数据会因为
     * 取不到令牌而全部转进断链缓存，白白丢掉一次实时上报机会 */
    if (g_cfg.q2 != NULL) {
        ring_queue_wakeup_all(g_cfg.q2);   /* 唤醒阻塞在取数据上的分发线程 */
    }
}

int dispatcher_join(void)
{
    uint32_t i = 0;

    if (g_started == 0) {
        return IOT_OK;
    }

    /* 1) 等分发线程把 Q2 抽干后自然退出（此时令牌还在补充，能正常发出去） */
    for (i = 0; i < g_worker_num; i++) {
        pthread_join(g_tids[i], NULL);
    }

    /* 2) worker 都退出了，这才让时钟线程停下，并放掉可能还卡在等令牌上的线程 */
    g_tick_stop = 1;
    if (g_outbound_bucket != NULL) {
        tbf_shutdown(g_outbound_bucket);
    }
    if (g_retry_bucket != NULL) {
        tbf_shutdown(g_retry_bucket);
    }

    /* 3) 回收时钟线程（最多再睡 TBF_TICK_MS 就退出）与监控线程 */
    if (g_tick_started != 0) {
        pthread_join(g_tick_tid, NULL);
        g_tick_started = 0;
    }
    if (g_mon_started != 0) {
        pthread_join(g_mon_tid, NULL);
        g_mon_started = 0;
    }

    if (g_unicast_fd >= 0) {
        unicast_sender_close(g_unicast_fd);
        g_unicast_fd = -1;
    }
    if (g_mcast_fd >= 0) {
        close(g_mcast_fd);
        g_mcast_fd = -1;
    }
    if (g_cfg.cache_path != NULL) {
        cache_writer_close();
    }
    if (g_own_bucket != 0 && g_outbound_bucket != NULL) {
        tbf_destroy(g_outbound_bucket);
        g_outbound_bucket = NULL;
        g_own_bucket = 0;
    }

    g_worker_num = 0;
    g_started = 0;
    return IOT_OK;
}
