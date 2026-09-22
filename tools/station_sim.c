/*
 * station_sim.c  ——  空气质量微站模拟器（全通道自动异常版）
 *
 * 功能：同时模拟 UDP(单播/组播) + TCP 老设备，自动混合正常/异常数据
 * 用法：./station_sim [-n 站数] [-r Hz] [-h IP] [-p port] [-m mcast]
 *                        [-t threads] [-d sec] [-s seed] [-T tcp_ip:port]
 *
 * 内置异常比例（仅作用于普通帧，不可关闭）：
 *   90% 正常 | 3% 溢出 | 3% 3σ突变 | 2% 重复 | 2% 乱序
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <unistd.h>
#include <signal.h>
#include <time.h>
#include <math.h>
#include <errno.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <getopt.h>
#include <pthread.h>

/* ===== 协议帧定义 (与 iot_collector.h 保持一致) ===== */
#define FRAME_MAGIC_0   0xEB
#define FRAME_MAGIC_1   0x90
#define FRAME_TYPE_NORMAL     0
#define FRAME_TYPE_ALARM      1
#define FRAME_TYPE_HEARTBEAT  2
#define FRAME_TYPE_RETRANSMIT 3

typedef struct {
    uint8_t  magic[2];
    uint8_t  version;
    uint8_t  frame_type;
    uint16_t station_id;
    uint32_t timestamp_sec;
    uint32_t timestamp_ms;
    int16_t  temperature;
    uint16_t humidity;
    uint16_t pm25;
    uint16_t pm10;
    uint16_t co_ppb;
    uint16_t noise_db;
    uint16_t crc16;
} __attribute__((packed)) env_frame_t;

/* ===== CRC16-CCITT ===== */
static uint16_t crc16_ccitt(const uint8_t *data, size_t len)
{
    uint16_t crc = 0xFFFF;
    for (size_t i = 0; i < len; i++) {
        crc ^= (uint16_t)data[i] << 8;
        for (int j = 0; j < 8; j++) {
            if (crc & 0x8000) crc = (crc << 1) ^ 0x1021;
            else              crc <<= 1;
        }
    }
    return crc;
}

/* ===== 全局控制 ===== */
static volatile sig_atomic_t g_running = 1;
static uint64_t g_total_sent = 0;
static uint64_t g_total_fail = 0;
static pthread_mutex_t g_stats_lock = PTHREAD_MUTEX_INITIALIZER;

static void on_signal(int sig) { (void)sig; g_running = 0; }

/* ===== 辅助函数 ===== */
static inline uint16_t clamped_add(uint16_t base, int offset, uint16_t max_val)
{
    int tmp = (int)base + offset;
    if (tmp < 0) tmp = 0;
    if (tmp > (int)max_val) tmp = (int)max_val;
    return (uint16_t)tmp;
}

/* ===== 站点 Profile ===== */
typedef struct {
    uint16_t station_id;
    double   base_temp, base_humidity;
    uint16_t base_pm25, base_pm10, base_co, base_noise;
    double   temp_drift;
    env_frame_t last_frame;
    int         has_last_frame;
} station_profile_t;

static void profile_init(station_profile_t *p, uint16_t sid)
{
    memset(p, 0, sizeof(*p));
    p->station_id    = sid;
    p->base_temp     = 15.0 + (sid % 20) * 0.5;
    p->base_humidity = 40.0 + (sid % 30) * 1.0;
    p->base_pm25     = 20  + (sid % 80);
    p->base_pm10     = 40  + (sid % 100);
    p->base_co       = 100 + (sid % 400);
    p->base_noise    = 400 + (sid % 300);
}

/* ===== 填充正常帧 ===== */
static void fill_normal_frame(env_frame_t *f, station_profile_t *p, uint8_t ftype)
{
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);

    double temp = p->base_temp + p->temp_drift + (rand() % 20 - 10) * 0.1;
    if (temp < -40.0) temp = -40.0;
    if (temp > 80.0)  temp = 80.0;
    double hum = p->base_humidity + (rand() % 40 - 20) * 0.1;
    if (hum < 0.0)   hum = 0.0;
    if (hum > 100.0) hum = 100.0;

    uint16_t pm25  = clamped_add(p->base_pm25,  rand() % 20 - 10, 999);
    uint16_t pm10  = clamped_add(p->base_pm10,  rand() % 30 - 15, 999);
    uint16_t co    = clamped_add(p->base_co,    rand() % 50 - 25, 5000);
    uint16_t noise = clamped_add(p->base_noise, rand() % 40 - 20, 1500);

    p->temp_drift += (rand() % 10 - 5) * 0.01;
    if (p->temp_drift > 3.0)  p->temp_drift = 3.0;
    if (p->temp_drift < -3.0) p->temp_drift = -3.0;

    memset(f, 0, sizeof(*f));
    f->magic[0]      = FRAME_MAGIC_0;
    f->magic[1]      = FRAME_MAGIC_1;
    f->version       = 1;
    f->frame_type    = ftype;
    f->station_id    = htons(p->station_id);
    f->timestamp_sec = htonl((uint32_t)ts.tv_sec);
    f->timestamp_ms  = htonl((uint32_t)(ts.tv_nsec / 1000000));
    f->temperature   = htons((int16_t)(temp * 10));
    f->humidity      = htons((uint16_t)(hum * 10));
    f->pm25          = htons(pm25);
    f->pm10          = htons(pm10);
    f->co_ppb        = htons(co);
    f->noise_db      = htons(noise);

    size_t crc_len = sizeof(env_frame_t) - sizeof(uint16_t);
    f->crc16 = htons(crc16_ccitt((const uint8_t *)f, crc_len));

    p->last_frame = *f;
    p->has_last_frame = 1;
}

/* ===== 自动异常注入（90%正常/3%溢出/3%突变/2%重复/2%乱序）===== */
static void auto_inject(env_frame_t *f, station_profile_t *p)
{
    int dice = rand() % 100;
    if (dice < 90) return; /* 正常帧 */

    if (dice < 93) {
        int field = rand() % 5;
        switch (field) {
        case 0: f->pm25        = htons(65535); break;
        case 1: f->pm10        = htons(65535); break;
        case 2: f->temperature = htons(-999);  break;
        case 3: f->humidity    = htons(65535); break;
        case 4: f->noise_db    = htons(65535); break;
        }
    } else if (dice < 96) {
        uint16_t spike = p->base_pm25 + 500;
        if (spike > 999) spike = 999;
        f->pm25 = htons(spike);
    } else if (dice < 98) {
        if (p->has_last_frame) {
            f->timestamp_sec = p->last_frame.timestamp_sec;
            f->timestamp_ms  = p->last_frame.timestamp_ms;
        }
    } else {
        uint32_t sec = ntohl(f->timestamp_sec);
        f->timestamp_sec = htonl(sec + 20);
    }

    size_t crc_len = sizeof(env_frame_t) - sizeof(uint16_t);
    f->crc16 = htons(crc16_ccitt((const uint8_t *)f, crc_len));
}

/* ===================================================================
 *  UDP 发送线程（新设备）
 * =================================================================== */
typedef struct {
    int                  sock_fd;
    struct sockaddr_in  *dest_addr;
    station_profile_t   *profiles;
    int                  station_count;
    double               interval_sec;
} udp_thread_arg_t;

static void *udp_sender_thread(void *arg)
{
    udp_thread_arg_t *ta = (udp_thread_arg_t *)arg;
    int n = ta->station_count;
    station_profile_t *profs = ta->profiles;
    double slot = ta->interval_sec / n;

    while (g_running) {
        for (int i = 0; i < n && g_running; i++) {
            env_frame_t frame;
            int r = rand() % 100;
            uint8_t ftype = FRAME_TYPE_NORMAL;
            if (r < 3)       ftype = FRAME_TYPE_ALARM;
            else if (r < 5)  ftype = FRAME_TYPE_HEARTBEAT;

            fill_normal_frame(&frame, &profs[i], ftype);
            if (ftype == FRAME_TYPE_NORMAL) auto_inject(&frame, &profs[i]);

            ssize_t ret = sendto(ta->sock_fd, &frame, sizeof(frame), 0,
                                 (struct sockaddr *)ta->dest_addr,
                                 sizeof(*ta->dest_addr));
            pthread_mutex_lock(&g_stats_lock);
            if (ret == sizeof(frame)) g_total_sent++; else g_total_fail++;
            pthread_mutex_unlock(&g_stats_lock);

            if (slot > 0.001) {
                struct timespec ts_sleep;
                ts_sleep.tv_sec  = (time_t)slot;
                ts_sleep.tv_nsec = (long)((slot - (time_t)slot) * 1e9);
                nanosleep(&ts_sleep, NULL);
            }
        }
    }
    return NULL;
}

/* ===================================================================
 *  TCP 发送线程（老设备兼容，带指数退避重连）
 * =================================================================== */
typedef struct {
    char     ip[64];
    uint16_t port;
    double   interval_sec;   /* 老设备发送间隔，默认5秒 */
} tcp_sim_config_t;

typedef struct {
    station_profile_t *profile;
    tcp_sim_config_t  *cfg;
} tcp_thread_arg_t;

static void *tcp_sender_thread(void *arg)
{
    tcp_thread_arg_t *ta = (tcp_thread_arg_t *)arg;
    station_profile_t *prof = ta->profile;
    tcp_sim_config_t *cfg = ta->cfg;
    int sock = -1;
    int retry_delay = 1;

    while (g_running) {
        /* --- 连接阶段 --- */
        if (sock < 0) {
            sock = socket(AF_INET, SOCK_STREAM, 0);
            if (sock < 0) { sleep(retry_delay); continue; }

            struct sockaddr_in addr = {0};
            addr.sin_family = AF_INET;
            addr.sin_port   = htons(cfg->port);
            inet_pton(AF_INET, cfg->ip, &addr.sin_addr);

            if (connect(sock, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
                close(sock); sock = -1;
                fprintf(stderr, "[TCP-SIM] 连接 %s:%u 失败, %ds后重试\n",
                        cfg->ip, cfg->port, retry_delay);
                sleep(retry_delay);
                retry_delay = (retry_delay < 30) ? retry_delay * 2 : 30;
                continue;
            }
            fprintf(stderr, "[TCP-SIM] ✅ 已连接 %s:%u\n", cfg->ip, cfg->port);
            retry_delay = 1;
        }

        /* --- 发送阶段 --- */
        env_frame_t frame;
        fill_normal_frame(&frame, prof, FRAME_TYPE_NORMAL);
        auto_inject(&frame, prof);  /* TCP也走同一套异常注入 */

        ssize_t ret = send(sock, &frame, sizeof(frame), MSG_NOSIGNAL);
        if (ret != sizeof(frame)) {
            fprintf(stderr, "[TCP-SIM] 发送失败, 断开重连\n");
            close(sock); sock = -1;
            continue;
        }

        pthread_mutex_lock(&g_stats_lock);
        g_total_sent++;
        pthread_mutex_unlock(&g_stats_lock);

        struct timespec ts_sleep;
        ts_sleep.tv_sec  = (time_t)cfg->interval_sec;
        ts_sleep.tv_nsec = (long)((cfg->interval_sec - (time_t)cfg->interval_sec) * 1e9);
        nanosleep(&ts_sleep, NULL);
    }

    if (sock >= 0) close(sock);
    return NULL;
}

/* ===== 统计线程 ===== */
static void *stats_thread(void *arg)
{
    (void)arg;
    uint64_t last_sent = 0;
    while (g_running) {
        sleep(1);
        pthread_mutex_lock(&g_stats_lock);
        uint64_t cur = g_total_sent;
        uint64_t rate = cur - last_sent;
        last_sent = cur;
        pthread_mutex_unlock(&g_stats_lock);
        fprintf(stderr, "[SIM] sent=%lu  fail=%lu  rate=%lu/s\n",
                (unsigned long)cur, (unsigned long)g_total_fail, (unsigned long)rate);
    }
    return NULL;
}

/* ===== 用法 ===== */
static void usage(const char *prog)
{
    fprintf(stderr,
        "用法: %s [选项]\n"
        "  -n <num>       模拟站数 (默认 200)\n"
        "  -r <Hz>        每站UDP速率 (默认 0.1)\n"
        "  -h <IP>        UDP目标IP (默认 127.0.0.1)\n"
        "  -p <port>      UDP目标端口 (默认 6000)\n"
        "  -m <mcast>     使用组播发送\n"
        "  -t <threads>   UDP发送线程数 (默认 4)\n"
        "  -d <sec>       运行时长 (默认 0=永久)\n"
        "  -s <seed>      随机种子\n"
        "  -T <ip:port>   启用TCP老设备模拟 (如 127.0.0.1:7000)\n"
        "\n"
        "内置自动异常注入（无需参数）：\n"
        "  90%% 正常 | 3%% 溢出 | 3%% 突变 | 2%% 重复 | 2%% 乱序\n", prog);
}

int main(int argc, char *argv[])
{
    int    num_stations = 200;
    double rate_hz      = 0.1;
    char   dest_ip[64]  = "127.0.0.1";
    int    dest_port    = 6000;
    char   mcast_ip[64] = "";
    int    num_threads  = 4;
    int    duration_sec = 0;
    unsigned int seed   = 0;
    int    use_mcast    = 0;

    /* TCP 配置 */
    tcp_sim_config_t tcp_cfg = { .interval_sec = 5.0 };
    int tcp_enabled = 0;

    int opt;
    while ((opt = getopt(argc, argv, "n:r:h:p:m:t:d:s:T:")) != -1) {
        switch (opt) {
        case 'n': num_stations = atoi(optarg); break;
        case 'r': rate_hz = atof(optarg); break;
        case 'h': strncpy(dest_ip, optarg, sizeof(dest_ip)-1); break;
        case 'p': dest_port = atoi(optarg); break;
        case 'm': strncpy(mcast_ip, optarg, sizeof(mcast_ip)-1); use_mcast=1; break;
        case 't': num_threads = atoi(optarg); break;
        case 'd': duration_sec = atoi(optarg); break;
        case 's': seed = (unsigned)atoi(optarg); break;
        case 'T': {
            char *colon = strchr(optarg, ':');
            if (!colon) { fprintf(stderr, "-T 格式: IP:PORT\n"); return 1; }
            size_t ip_len = (size_t)(colon - optarg);
            if (ip_len >= sizeof(tcp_cfg.ip)) ip_len = sizeof(tcp_cfg.ip) - 1;
            memcpy(tcp_cfg.ip, optarg, ip_len);
            tcp_cfg.ip[ip_len] = '\0';
            tcp_cfg.port = (uint16_t)atoi(colon + 1);
            tcp_enabled = 1;
            break;
        }
        default: usage(argv[0]); return 1;
        }
    }

    if (num_stations <= 0 || num_stations > 10000) { fprintf(stderr, "站数范围: 1~10000\n"); return 1; }
    if (rate_hz <= 0) rate_hz = 0.1;
    if (num_threads <= 0) num_threads = 1;
    if (num_threads > num_stations) num_threads = num_stations;
    if (seed == 0) seed = (unsigned)time(NULL);
    srand(seed);

    double interval = 1.0 / rate_hz;

    fprintf(stderr,
        "============================\n"
        "  微站模拟器 (全通道自动异常版)\n"
        "  UDP: %d站 × %.2fHz → %s:%d %s\n"
        "  TCP: %s\n"
        "  异常: 3%%溢出 3%%突变 2%%重复 2%%乱序\n"
        "============================\n",
        num_stations, rate_hz, dest_ip, dest_port,
        use_mcast ? "(组播)" : "(单播)",
        tcp_enabled ? tcp_cfg.ip : "(未启用)");

    struct sigaction sa = {0};
    sa.sa_handler = on_signal;
    sigemptyset(&sa.sa_mask);
    sigaction(SIGINT, &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);
    signal(SIGPIPE, SIG_IGN);

    /* ===== UDP Socket ===== */
    int sock = socket(AF_INET, SOCK_DGRAM, 0);
    if (sock < 0) { perror("socket"); return 1; }
    int sndbuf = 4*1024*1024;
    setsockopt(sock, SOL_SOCKET, SO_SNDBUF, &sndbuf, sizeof(sndbuf));

    struct sockaddr_in dest_addr;
    memset(&dest_addr, 0, sizeof(dest_addr));
    dest_addr.sin_family = AF_INET;
    dest_addr.sin_port   = htons((uint16_t)dest_port);
    if (use_mcast) {
        inet_pton(AF_INET, mcast_ip, &dest_addr.sin_addr);
        uint8_t ttl = 16;
        setsockopt(sock, IPPROTO_IP, IP_MULTICAST_TTL, &ttl, sizeof(ttl));
        struct in_addr local_if = {.s_addr = htonl(INADDR_ANY)};
        setsockopt(sock, IPPROTO_IP, IP_MULTICAST_IF, &local_if, sizeof(local_if));
    } else {
        inet_pton(AF_INET, dest_ip, &dest_addr.sin_addr);
    }

    /* ===== Profiles ===== */
    station_profile_t *profiles = calloc(num_stations, sizeof(station_profile_t));
    if (!profiles) { perror("calloc"); close(sock); return 1; }
    for (int i = 0; i < num_stations; i++)
        profile_init(&profiles[i], (uint16_t)(i+1));

    /* ===== 统计线程 ===== */
    pthread_t stats_tid;
    pthread_create(&stats_tid, NULL, stats_thread, NULL);

    /* ===== UDP 发送线程 ===== */
    int per_thread = num_stations / num_threads;
    int remainder  = num_stations % num_threads;
    pthread_t *tids = calloc(num_threads, sizeof(pthread_t));
    udp_thread_arg_t *args = calloc(num_threads, sizeof(udp_thread_arg_t));

    int offset = 0;
    for (int t = 0; t < num_threads; t++) {
        int cnt = per_thread + (t < remainder ? 1 : 0);
        args[t].sock_fd       = sock;
        args[t].dest_addr     = &dest_addr;
        args[t].profiles      = &profiles[offset];
        args[t].station_count = cnt;
        args[t].interval_sec  = interval;
        pthread_create(&tids[t], NULL, udp_sender_thread, &args[t]);
        offset += cnt;
    }

    /* ===== TCP 老设备线程（可选）===== */
    pthread_t tcp_tid;
    station_profile_t tcp_profile;
    tcp_thread_arg_t tcp_arg;
    if (tcp_enabled) {
        profile_init(&tcp_profile, 9999);  /* station_id=9999 标识TCP老设备 */
        tcp_arg.profile = &tcp_profile;
        tcp_arg.cfg     = &tcp_cfg;
        pthread_create(&tcp_tid, NULL, tcp_sender_thread, &tcp_arg);
        fprintf(stderr, "[TCP-SIM] 老设备模拟已启动 → %s:%u (间隔%.1fs)\n",
                tcp_cfg.ip, tcp_cfg.port, tcp_cfg.interval_sec);
    }

    /* ===== 主循环等待 ===== */
    if (duration_sec > 0) {
        for (int s = 0; s < duration_sec && g_running; s++) sleep(1);
        g_running = 0;
    } else {
        while (g_running) sleep(1);
    }

    /* ===== 优雅退出 ===== */
    fprintf(stderr, "[SIM] 正在停止...\n");
    for (int t = 0; t < num_threads; t++) pthread_join(tids[t], NULL);
    if (tcp_enabled) pthread_join(tcp_tid, NULL);
    pthread_join(stats_tid, NULL);

    fprintf(stderr, "总发送: %lu  总失败: %lu\n",
            (unsigned long)g_total_sent, (unsigned long)g_total_fail);

    close(sock);
    free(profiles);
    free(tids);
    free(args);
    return 0;
}
