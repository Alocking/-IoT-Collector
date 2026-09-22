/*
 * frame_decoder.c  ——  协议帧解析打印工具
 *
 * 功能：监听 UDP 端口, 捕获并解析 env_frame_t 协议帧,
 *       彩色打印各字段 + CRC校验结果 + 统计信息
 * 用法：./frame_decoder [-p 端口] [-m 组播IP] [-i 网卡] [-l 日志文件]
 *                        [-f 从文件读取] [-c 最大帧数]
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <unistd.h>
#include <signal.h>
#include <time.h>
#include <errno.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <net/if.h>
#include <getopt.h>

/* ===== 协议帧定义 (与 iot_collector.h 保持一致) ===== */
#define FRAME_MAGIC_0   0xEB
#define FRAME_MAGIC_1   0x90

#define FRAME_TYPE_NORMAL      0
#define FRAME_TYPE_ALARM       1
#define FRAME_TYPE_HEARTBEAT   2
#define FRAME_TYPE_RETRANSMIT  3

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
            if (crc & 0x8000)
                crc = (crc << 1) ^ 0x1021;
            else
                crc <<= 1;
        }
    }
    return crc;
}

/* ===== 帧类型字符串 ===== */
static const char *frame_type_str(uint8_t type)
{
    switch (type) {
    case FRAME_TYPE_NORMAL:     return "普通数据";
    case FRAME_TYPE_ALARM:      return "⚠ 告警";
    case FRAME_TYPE_HEARTBEAT:  return "心跳";
    case FRAME_TYPE_RETRANSMIT: return "补传";
    default:                    return "未知";
    }
}

/* ===== ANSI 颜色 ===== */
#define CLR_RESET   "\033[0m"
#define CLR_RED     "\033[1;31m"
#define CLR_GREEN   "\033[1;32m"
#define CLR_YELLOW  "\033[1;33m"
#define CLR_BLUE    "\033[1;34m"
#define CLR_CYAN    "\033[1;36m"
#define CLR_MAGENTA "\033[1;35m"
#define CLR_WHITE   "\033[1;37m"
#define CLR_DIM     "\033[2m"

/* ===== 全局状态 ===== */
static volatile sig_atomic_t g_running = 1;
static uint64_t g_recv_count  = 0;
static uint64_t g_crc_ok      = 0;
static uint64_t g_crc_fail    = 0;
static uint64_t g_magic_fail  = 0;
static uint64_t g_size_fail   = 0;
static uint64_t g_total_bytes = 0;
static uint64_t g_type_count[256] = {0};

static void on_signal(int sig) {
    (void)sig;
    g_running = 0;
}

/* ===== 格式化时间戳 (毫秒级) ===== */
static void format_timestamp(uint32_t sec, uint32_t msec, char *buf, size_t sz)
{
    time_t t = (time_t)sec;
    struct tm tm;
    localtime_r(&t, &tm);
    snprintf(buf, sz, "%04d-%02d-%02d %02d:%02d:%02d.%03u",
             tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday,
             tm.tm_hour, tm.tm_min, tm.tm_sec, msec);
}

/* ===== 解析并打印一帧 ===== */
static void decode_frame(const uint8_t *raw, ssize_t len,
                         const struct sockaddr_in *src, FILE *logfp)
{
    char ts_buf[64];
    char src_ip[INET_ADDRSTRLEN];
    inet_ntop(AF_INET, &src->sin_addr, src_ip, sizeof(src_ip));

    if ((size_t)len < sizeof(env_frame_t)) {
        fprintf(stderr, CLR_RED "[ERR] 帧长度不足: %zd < %zu 来自 %s:%d" CLR_RESET "\n",
                len, sizeof(env_frame_t), src_ip, ntohs(src->sin_port));
        g_size_fail++;
        return;
    }

    const env_frame_t *f = (const env_frame_t *)raw;

    if (f->magic[0] != FRAME_MAGIC_0 || f->magic[1] != FRAME_MAGIC_1) {
        fprintf(stderr, CLR_RED "[ERR] Magic错误: 0x%02X 0x%02X (期望 0xEB 0x90) 来自 %s" CLR_RESET "\n",
                f->magic[0], f->magic[1], src_ip);
        g_magic_fail++;
        return;
    }

    uint16_t sid   = ntohs(f->station_id);
    uint32_t ts_s  = ntohl(f->timestamp_sec);
    uint32_t ts_ms = ntohl(f->timestamp_ms);
    int16_t  temp  = (int16_t)ntohs((uint16_t)f->temperature);
    uint16_t hum   = ntohs(f->humidity);
    uint16_t pm25  = ntohs(f->pm25);
    uint16_t pm10  = ntohs(f->pm10);
    uint16_t co    = ntohs(f->co_ppb);
    uint16_t noise = ntohs(f->noise_db);
    uint16_t crc_r = ntohs(f->crc16);

    size_t crc_len = sizeof(env_frame_t) - sizeof(uint16_t);
    uint16_t crc_calc = crc16_ccitt(raw, crc_len);
    int crc_ok = (crc_calc == crc_r);

    if (crc_ok) g_crc_ok++; else g_crc_fail++;
    g_type_count[f->frame_type]++;

    format_timestamp(ts_s, ts_ms, ts_buf, sizeof(ts_buf));

    const char *type_color = CLR_GREEN;
    if (f->frame_type == FRAME_TYPE_ALARM)          type_color = CLR_RED;
    else if (f->frame_type == FRAME_TYPE_HEARTBEAT)  type_color = CLR_CYAN;
    else if (f->frame_type == FRAME_TYPE_RETRANSMIT) type_color = CLR_YELLOW;

    printf(CLR_DIM "───────────────────────────────────────────────────────────" CLR_RESET "\n");
    printf(CLR_WHITE "📦 帧 #%lu" CLR_RESET "  来源: %s:%d  大小: %zdB\n",
           (unsigned long)g_recv_count, src_ip, ntohs(src->sin_port), len);

    printf("  " CLR_BLUE "帧头" CLR_RESET ": 0x%02X 0x%02X  "
           CLR_BLUE "版本" CLR_RESET ": %u  "
           CLR_BLUE "类型" CLR_RESET ": %s[%s]%s\n",
           f->magic[0], f->magic[1], f->version,
           type_color, frame_type_str(f->frame_type), CLR_RESET);

    printf("  " CLR_BLUE "站号" CLR_RESET ": %-5u  "
           CLR_BLUE "时间" CLR_RESET ": %s\n", sid, ts_buf);

    printf("  ┌──────────┬──────────┬──────────┬──────────┬──────────┬──────────┐\n");
    printf("  │ " CLR_MAGENTA "温度" CLR_RESET "   │ " CLR_MAGENTA "湿度" CLR_RESET "   │ "
           CLR_MAGENTA "PM2.5" CLR_RESET "  │ " CLR_MAGENTA "PM10" CLR_RESET "   │ "
           CLR_MAGENTA "CO" CLR_RESET "     │ " CLR_MAGENTA "噪声" CLR_RESET "   │\n");
    printf("  │ %6.1f℃ │ %5.1f%%  │ %5u    │ %5u    │ %5u    │ %5.1fdB │\n",
           temp / 10.0, hum / 10.0, pm25, pm10, co, noise / 10.0);
    printf("  └──────────┴──────────┴──────────┴──────────┴──────────┴──────────┘\n");

    printf("  " CLR_BLUE "CRC16" CLR_RESET ": 0x%04X (计算: 0x%04X) → %s%s%s\n",
           crc_r, crc_calc,
           crc_ok ? CLR_GREEN : CLR_RED,
           crc_ok ? "✓ 校验通过" : "✗ 校验失败!",
           CLR_RESET);

    if (pm25 > 500)
        printf("  " CLR_RED "⚠ PM2.5 严重超标: %u μg/m³" CLR_RESET "\n", pm25);
    if (temp > 600 || temp < -400)
        printf("  " CLR_RED "⚠ 温度异常: %.1f℃" CLR_RESET "\n", temp / 10.0);

    if (logfp) {
        fprintf(logfp, "%lu,%s,%u,%u,%s,%.1f,%.1f,%u,%u,%u,%.1f,%s\n",
                (unsigned long)g_recv_count, ts_buf, sid, f->frame_type,
                frame_type_str(f->frame_type),
                temp / 10.0, hum / 10.0, pm25, pm10, co, noise / 10.0,
                crc_ok ? "OK" : "FAIL");
        fflush(logfp);
    }
}

/* ===== 统计打印 ===== */
static void print_summary(void)
{
    printf("\n" CLR_WHITE "========================================" CLR_RESET "\n");
    printf(CLR_WHITE "  📊 解析统计汇总" CLR_RESET "\n");
    printf(CLR_WHITE "========================================" CLR_RESET "\n");
    printf("  总接收:  %lu 帧  (%lu 字节)\n",
           (unsigned long)g_recv_count, (unsigned long)g_total_bytes);
    printf("  CRC通过: %lu  " CLR_RED "CRC失败: %lu" CLR_RESET "\n",
           (unsigned long)g_crc_ok, (unsigned long)g_crc_fail);
    printf("  " CLR_RED "Magic错误: %lu  长度异常: %lu" CLR_RESET "\n",
           (unsigned long)g_magic_fail, (unsigned long)g_size_fail);
    printf("  帧类型分布:\n");
    for (int i = 0; i < 256; i++) {
        if (g_type_count[i] > 0) {
            printf("    [%d] %-8s : %lu\n", i, frame_type_str((uint8_t)i),
                   (unsigned long)g_type_count[i]);
        }
    }
    printf(CLR_WHITE "========================================" CLR_RESET "\n");
}

/* ===== 用法 ===== */
static void usage(const char *prog)
{
    fprintf(stderr,
        "用法: %s [选项]\n"
        "  -p <port>     监听端口 (默认 6001, ✅ 避开采集中心的6000)\n"  /* ✅ 修改 */
        "  -m <mcastIP>  加入组播组 (如 239.10.10.10)\n"
        "  -i <iface>    网卡名 (默认 eth0, 用 'ip addr' 查看实际名称)\n" /* ✅ 修改 */
        "  -l <file>     输出CSV日志文件\n"
        "  -f <file>     从二进制文件读取帧 (而非网络)\n"
        "  -c <num>      最大帧数, 0=无限 (默认 0)\n",
        prog);
}

int main(int argc, char *argv[])
{
    int    listen_port = 6001;       /* ✅ 修改: 默认端口改为6001，避免与采集中心6000冲突 */
    char   mcast_ip[64] = "";
    char   iface[32]   = "eth0";
    char   log_file[256] = "";
    char   bin_file[256] = "";
    int    max_frames   = 0;
    int    use_mcast    = 0;
    int    from_file    = 0;

    int opt;
    while ((opt = getopt(argc, argv, "p:m:i:l:f:c:")) != -1) {
        switch (opt) {
        case 'p': listen_port = atoi(optarg); break;
        case 'm': strncpy(mcast_ip, optarg, sizeof(mcast_ip)-1); use_mcast=1; break;
        case 'i': strncpy(iface, optarg, sizeof(iface)-1); break;
        case 'l': strncpy(log_file, optarg, sizeof(log_file)-1); break;
        case 'f': strncpy(bin_file, optarg, sizeof(bin_file)-1); from_file=1; break;
        case 'c': max_frames = atoi(optarg); break;
        default:  usage(argv[0]); return 1;
        }
    }

    struct sigaction sa = {0};
    sa.sa_handler = on_signal;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;
    sigaction(SIGINT,  &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);

    FILE *logfp = NULL;
    if (log_file[0]) {
        logfp = fopen(log_file, "w");
        if (!logfp) { perror("fopen log"); return 1; }
        fprintf(logfp, "seq,timestamp,station_id,frame_type,type_name,"
                       "temperature,humidity,pm25,pm10,co,noise,crc\n");
    }

    fprintf(stderr, CLR_CYAN
        "============================\n"
        "  协议帧解析打印工具 启动\n"
        "  帧大小: %zu 字节\n"
        "============================\n" CLR_RESET,
        sizeof(env_frame_t));

    if (from_file) {
        FILE *fp = fopen(bin_file, "rb");
        if (!fp) { perror("fopen bin"); return 1; }
        fprintf(stderr, "[DEC] 从文件 %s 读取帧...\n", bin_file);

        uint8_t buf[4096];
        struct sockaddr_in fake_src;
        memset(&fake_src, 0, sizeof(fake_src));
        fake_src.sin_family = AF_INET;
        inet_pton(AF_INET, "127.0.0.1", &fake_src.sin_addr);
        fake_src.sin_port = htons(0);

        while (g_running) {
            ssize_t n = (ssize_t)fread(buf, 1, sizeof(env_frame_t), fp);
            if (n <= 0) break;
            g_recv_count++;
            g_total_bytes += (uint64_t)n;
            decode_frame(buf, n, &fake_src, logfp);
            if (max_frames > 0 && (int)g_recv_count >= max_frames) break;
        }
        fclose(fp);
    } else {
        int sock = socket(AF_INET, SOCK_DGRAM, 0);
        if (sock < 0) { perror("socket"); return 1; }

        int reuse = 1;
        setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));

        int rcvbuf = 4 * 1024 * 1024;
        setsockopt(sock, SOL_SOCKET, SO_RCVBUF, &rcvbuf, sizeof(rcvbuf));

        struct sockaddr_in bind_addr;
        memset(&bind_addr, 0, sizeof(bind_addr));
        bind_addr.sin_family      = AF_INET;
        bind_addr.sin_port        = htons((uint16_t)listen_port);
        bind_addr.sin_addr.s_addr = htonl(INADDR_ANY);

        if (bind(sock, (struct sockaddr *)&bind_addr, sizeof(bind_addr)) < 0) {
            perror("bind");
            close(sock);
            return 1;
        }

        if (use_mcast) {
            struct ip_mreqn mreq;
            memset(&mreq, 0, sizeof(mreq));
            inet_pton(AF_INET, mcast_ip, &mreq.imr_multiaddr);
            mreq.imr_address.s_addr = htonl(INADDR_ANY);
            mreq.imr_ifindex = (int)if_nametoindex(iface);

            if (setsockopt(sock, IPPROTO_IP, IP_ADD_MEMBERSHIP,
                           &mreq, sizeof(mreq)) < 0) {
                perror("IP_ADD_MEMBERSHIP");
            } else {
                fprintf(stderr, "[DEC] 已加入组播组 %s (接口 %s)\n", mcast_ip, iface);

                /* ✅ 修改: 开启组播本地回环，确保本机能收到自己发出的组播包 */
                uint8_t loopback = 1;
                if (setsockopt(sock, IPPROTO_IP, IP_MULTICAST_LOOP,
                               &loopback, sizeof(loopback)) < 0) {
                    perror("IP_MULTICAST_LOOP");
                } else {
                    fprintf(stderr, "[DEC] 组播本地回环已开启\n");
                }
            }
        }

        fprintf(stderr, "[DEC] 监听 UDP 端口 %d ...\n", listen_port);

        uint8_t buf[4096];
        struct sockaddr_in src_addr;
        socklen_t src_len;

        while (g_running) {
            src_len = sizeof(src_addr);
            ssize_t n = recvfrom(sock, buf, sizeof(buf), 0,
                                 (struct sockaddr *)&src_addr, &src_len);
            if (n < 0) {
                if (errno == EINTR) continue;
                perror("recvfrom");
                break;
            }
            g_recv_count++;
            g_total_bytes += (uint64_t)n;
            decode_frame(buf, n, &src_addr, logfp);
            if (max_frames > 0 && (int)g_recv_count >= max_frames) break;
        }

        if (use_mcast) {
            struct ip_mreqn mreq;
            memset(&mreq, 0, sizeof(mreq));
            inet_pton(AF_INET, mcast_ip, &mreq.imr_multiaddr);
            mreq.imr_ifindex = (int)if_nametoindex(iface);
            setsockopt(sock, IPPROTO_IP, IP_DROP_MEMBERSHIP, &mreq, sizeof(mreq));
        }
        close(sock);
    }

    print_summary();

    if (logfp) {
        fclose(logfp);
        fprintf(stderr, "[DEC] CSV日志已写入: %s\n", log_file);
    }

    return 0;
}
