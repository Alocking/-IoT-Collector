//云端桥接 cloud_bridge(路径二) — 计划书§7.2 监管平台 + ES-LINK HTTP 上报
// 1) 本进程以 TCP Server 角色充当「监管平台」，与 iot-collector -p 127.0.0.1:PORT 对接
// 2) 收到的是线路字节（网络序 EB90 帧 + 心跳），用 frame_decode 还原主机序
// 3) 将温度/湿度/PM 等映射到云端 device_id + sensor_id，HTTP POST 上报
// 4) --dry-run：只解析打印，不访问外网，便于离线验收流水线
// 5) 凭据放在 cloud_bridge.conf（勿提交仓库）；缺失则自动退回 dry-run
// 6) 退出：Ctrl+C → g_run=0 → close listen/conn → 统计打印
#include "iot_collector.h"//项目总头（协议/帧）
#include <getopt.h>
#include <poll.h>
#include <fcntl.h>
#include <sys/stat.h>

#define BRIDGE_DEFAULT_PORT 9000
#define BRIDGE_HTTP_PORT 80
#define BRIDGE_MAX_SENSORS 8
#define BRIDGE_BATCH_MAX 32

static volatile sig_atomic_t g_run = 1;//运行标志

//云端传感器映射：字段名 + 云端 sensor_id
typedef struct {
 char field[16]; /* temp/humi/pm25/pm10/co/noise */
 char sensor_id[32];
 int enabled;
} cloud_sensor_t;

static char g_cloud_ip[64] = "119.29.98.16";//平台 IP（SDK 演示地址，可被 conf 覆盖）
static char g_device_id[32] = "";//云端设备号
static char g_api_key[512] = "";//API-KEY（JWT）
static int g_dry_run = 1;//默认干跑：未配置凭据时不访问外网
static uint16_t g_listen_port = BRIDGE_DEFAULT_PORT;

static cloud_sensor_t g_sensors[BRIDGE_MAX_SENSORS];
static int g_sensor_n = 0;

static uint64_t g_frames = 0;//收到的完整帧
static uint64_t g_hearts = 0;//心跳帧
static uint64_t g_upload_ok = 0;//上报成功
static uint64_t g_upload_fail = 0;//上报失败
static uint64_t g_decode_fail = 0;//解码失败

static void on_sig(int s) { (void)s; g_run = 0; }//信号：只改标志

//Base64 编码（HTTP Basic Auth；与 iot_v1.1 SDK 目标一致）
static int b64_encode(const unsigned char *in, size_t in_len, char *out, size_t out_cap)
{
 static const char tbl[] =
 "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
 size_t i = 0, o = 0;
 if (in == NULL || out == NULL) {
 return -1;
 }
 while (i < in_len) {
 size_t remain = in_len - i;
 unsigned int b0 = in[i];
 unsigned int b1 = (remain > 1) ? in[i + 1] : 0;
 unsigned int b2 = (remain > 2) ? in[i + 2] : 0;
 unsigned int v = (b0 << 16) | (b1 << 8) | b2;
 if (o + 4 >= out_cap) {
 return -1;
 }
 out[o++] = tbl[(v >> 18) & 63];
 out[o++] = tbl[(v >> 12) & 63];
 out[o++] = (remain > 1) ? tbl[(v >> 6) & 63] : '=';
 out[o++] = (remain > 2) ? tbl[v & 63] : '=';
 i += (remain > 3) ? 3 : remain;
 if (remain <= 3) {
 i = in_len;
 }
 }
 out[o] = '\0';
 return (int)o;
}

//加载 cloud_bridge.conf：key=value，# 注释
static int load_conf(const char *path)
{
 char line[640];
 FILE *fp = fopen(path, "r");
 if (fp == NULL) {
 return -1;//无配置文件
 }
 while (fgets(line, sizeof(line), fp) != NULL) {
 char *eq = strchr(line, '=');
 char *nl;
 if (line[0] == '#' || line[0] == '\n' || line[0] == '\r') continue;
 if (eq == NULL) continue;
 *eq = '\0';
 nl = strchr(eq + 1, '\n');
 if (nl) *nl = '\0';
 nl = strchr(eq + 1, '\r');
 if (nl) *nl = '\0';
 if (strcmp(line, "CLOUD_IP") == 0) {
 strncpy(g_cloud_ip, eq + 1, sizeof(g_cloud_ip) - 1);
 } else if (strcmp(line, "DEVICE_ID") == 0) {
 strncpy(g_device_id, eq + 1, sizeof(g_device_id) - 1);
 } else if (strcmp(line, "API_KEY") == 0) {
 strncpy(g_api_key, eq + 1, sizeof(g_api_key) - 1);
 } else if (strcmp(line, "LISTEN_PORT") == 0) {
 g_listen_port = (uint16_t)atoi(eq + 1);
 } else if (strncmp(line, "SENSOR_", 7) == 0 && g_sensor_n < BRIDGE_MAX_SENSORS) {
 /* SENSOR_TEMP=12 → field 存 temp（统一小写），sensor_id=12 */
 const char *raw = line + 7;
 char field_l[16];
 size_t k = 0;
 for (; raw[k] != '\0' && k < sizeof(field_l) - 1; k++) {
 char c = raw[k];
 if (c >= 'A' && c <= 'Z') {
 c = (char)(c - 'A' + 'a');
 }
 field_l[k] = c;
 }
 field_l[k] = '\0';
 strncpy(g_sensors[g_sensor_n].field, field_l, sizeof(g_sensors[0].field) - 1);
 g_sensors[g_sensor_n].field[sizeof(g_sensors[0].field) - 1] = '\0';
 strncpy(g_sensors[g_sensor_n].sensor_id, eq + 1, sizeof(g_sensors[0].sensor_id) - 1);
 g_sensors[g_sensor_n].sensor_id[sizeof(g_sensors[0].sensor_id) - 1] = '\0';
 g_sensors[g_sensor_n].enabled = 1;
 g_sensor_n++;
 }
 }
 fclose(fp);
 if (g_device_id[0] != '\0' && g_api_key[0] != '\0' && g_sensor_n > 0) {
 g_dry_run = 0;//凭据齐全才真正上云
 }
 return 0;
}

//从帧字段取数值（字段名大小写不敏感：TEMP/temp 均可）
static int frame_field_value(const env_frame_t *f, const char *field, double *out)
{
 char key[16];
 size_t i = 0;
 if (field == NULL || out == NULL) {
 return -1;
 }
 /* 归一成小写再比较，兼容 conf 里 SENSOR_TEMP / SENSOR_temp */
 for (i = 0; field[i] != '\0' && i < sizeof(key) - 1; i++) {
 char c = field[i];
 if (c >= 'A' && c <= 'Z') {
 c = (char)(c - 'A' + 'a');
 }
 key[i] = c;
 }
 key[i] = '\0';

 if (strcmp(key, "temp") == 0) { *out = f->temperature / 10.0; return 0; }
 if (strcmp(key, "humi") == 0) { *out = f->humidity / 10.0; return 0; }
 if (strcmp(key, "pm25") == 0) { *out = (double)f->pm25; return 0; }
 if (strcmp(key, "pm10") == 0) { *out = (double)f->pm10; return 0; }
 if (strcmp(key, "co") == 0) { *out = (double)f->co_ppb; return 0; }
 if (strcmp(key, "noise") == 0) { *out = f->noise_db / 10.0; return 0; }
 return -1;
}

//HTTP 上报单个传感器：POST /api/1.0/device/{d}/sensor/{s}/data
static int http_upload_one(const char *sensor_id, double value)
{
 char auth_raw[560];
 char auth_b64[760];
 char body[64];
 char req[1400];
 struct sockaddr_in addr;
 int sd = -1;
 int n = 0;
 int ret = -1;

 if (g_cloud_ip[0] == '\0' || g_device_id[0] == '\0' ||
 g_api_key[0] == '\0' || sensor_id[0] == '\0') {
 return -1;
 }

 snprintf(auth_raw, sizeof(auth_raw), "%s:", g_api_key);
 if (b64_encode((const unsigned char *)auth_raw, strlen(auth_raw),
 auth_b64, sizeof(auth_b64)) < 0) {
 return -1;
 }
 snprintf(body, sizeof(body), "{\"data\":%f}", value);
 /* 与 iot_v1.1/sdk_for_linux/demon/iot.c 相同路径与头格式 */
 n = snprintf(req, sizeof(req),
 "POST /api/1.0/device/%s/sensor/%s/data HTTP/1.1\r\n"
 "Host: www.embsky.com\r\n"
 "Accept: */*\r\n"
 "Authorization: Basic %s\r\n"
 "Content-Length: %d\r\n"
 "Content-Type: application/json;charset=utf-8\r\n"
 "Connection: close\r\n"
 "\r\n"
 "%s\r\n",
 g_device_id, sensor_id, auth_b64, (int)strlen(body), body);
 if (n <= 0 || n >= (int)sizeof(req)) {
 return -1;
 }

 memset(&addr, 0, sizeof(addr));
 addr.sin_family = AF_INET;
 addr.sin_port = htons(BRIDGE_HTTP_PORT);
 addr.sin_addr.s_addr = inet_addr(g_cloud_ip);//SDK 同款：IPv4 点分地址

 sd = socket(AF_INET, SOCK_STREAM, 0);
 if (sd < 0) {
 return -1;
 }
 if (connect(sd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
 close(sd);
 return -1;
 }
 if (write(sd, req, strlen(req)) < 0) {
 close(sd);
 return -1;
 }
 /* 读一小段响应即可，不必完整解析 */
 {
 char resp[256];
 ssize_t rn = read(sd, resp, sizeof(resp) - 1);
 if (rn > 0) {
 resp[rn] = '\0';
 if (strncmp(resp, "HTTP/1.", 7) == 0) {
 /* 200/201 视为成功；401/404 等记失败 */
 if (strstr(resp, " 200") != NULL || strstr(resp, " 201") != NULL) {
 ret = 0;
 } else {
 fprintf(stderr, "[bridge] HTTP resp: %.80s\n", resp);
 ret = -1;
 }
 }
 }
 }
 close(sd);
 return ret;
}

//处理一帧：打印 + 按映射上报
static void handle_frame(const env_frame_t *f)
{
 if (f->frame_type == FRAME_HEART) {
 g_hearts++;
 return;//心跳不上云
 }
 g_frames++;

 printf("[bridge] frame station=%u type=%u temp=%.1f humi=%.1f pm25=%u pm10=%u co=%u noise=%.1f ts=%u\n",
 f->station_id, f->frame_type,
 f->temperature / 10.0, f->humidity / 10.0,
 f->pm25, f->pm10, f->co_ppb, f->noise_db / 10.0,
 f->timestamp_sec);

 if (g_dry_run) {
 return;//离线模式：只观察，不访问云端
 }

 for (int i = 0; i < g_sensor_n; i++) {
 double v = 0.0;
 if (!g_sensors[i].enabled) continue;
 if (frame_field_value(f, g_sensors[i].field, &v) != 0) continue;
 if (http_upload_one(g_sensors[i].sensor_id, v) == 0) {
 g_upload_ok++;
 } else {
 g_upload_fail++;
 }
 }
}

//从 TCP 字节流切出完整帧并处理（与 collector decoder 同协议）
typedef struct {
 uint8_t buf[ENV_FRAME_WIRE_SIZE * 8];
 size_t len;
} stream_t;

static void stream_feed(stream_t *st, const uint8_t *data, size_t n)
{
 size_t i = 0;
 while (i < n) {
 size_t room = sizeof(st->buf) - st->len;
 size_t take = (n - i < room) ? (n - i) : room;
 if (take == 0) {
 /* 缓冲满：丢 1 字节重同步 */
 memmove(st->buf, st->buf + 1, st->len - 1);
 st->len--;
 continue;
 }
 memcpy(st->buf + st->len, data + i, take);
 st->len += take;
 i += take;
 }
 while (st->len >= ENV_FRAME_WIRE_SIZE) {
 env_frame_t frame;
 if (frame_magic_ok(st->buf, ENV_FRAME_WIRE_SIZE) == 0 ||
 frame_decode(st->buf, ENV_FRAME_WIRE_SIZE, &frame) != 0) {
 memmove(st->buf, st->buf + 1, st->len - 1);
 st->len--;
 g_decode_fail++;
 continue;
 }
 handle_frame(&frame);
 memmove(st->buf, st->buf + ENV_FRAME_WIRE_SIZE,
 st->len - ENV_FRAME_WIRE_SIZE);
 st->len -= ENV_FRAME_WIRE_SIZE;
 }
}

static void print_usage(const char *prog)
{
 fprintf(stderr,
 "Usage: %s [options]\n"
 " -l <port> 监听端口（默认 %d，对应 iot-collector -p 127.0.0.1:port）\n"
 " -c <file> 配置文件（默认 ./cloud_bridge.conf）\n"
 " -d 强制 dry-run（只打印，不上云）\n"
 " -h 帮助\n"
 "\n"
 "conf 示例:\n"
 " CLOUD_IP=119.29.98.16\n"
 " DEVICE_ID=你的设备号\n"
 " API_KEY=你的APIKEY\n"
 " SENSOR_TEMP=传感器id\n"
 " SENSOR_PM25=传感器id\n"
 "\n"
 "离线联调:\n"
 " ./cloud_bridge -l 9000 -d\n"
 " ./iot-collector -s -p 127.0.0.1:9000\n",
 prog, BRIDGE_DEFAULT_PORT);
}

int main(int argc, char **argv)
{
 const char *conf_path = "./cloud_bridge.conf";
 int force_dry = 0;
 int listen_fd = -1;
 int conn_fd = -1;
 int opt;
 int reuse = 1;
 struct sockaddr_in addr;
 struct pollfd pfd[2];
 stream_t st;

 while ((opt = getopt(argc, argv, "l:c:dh")) != -1) {
 switch (opt) {
 case 'l': g_listen_port = (uint16_t)atoi(optarg); break;
 case 'c': conf_path = optarg; break;
 case 'd': force_dry = 1; break;
 case 'h':
 default:
 print_usage(argv[0]);
 return (opt == 'h') ? 0 : 1;
 }
 }

 if (load_conf(conf_path) == 0) {
 printf("[bridge] loaded conf %s device=%s sensors=%d mode=%s\n",
 conf_path,
 g_device_id[0] ? g_device_id : "(none)",
 g_sensor_n,
 g_dry_run ? "DRY-RUN" : "CLOUD");
 } else {
 printf("[bridge] no conf (%s), stay DRY-RUN\n", conf_path);
 g_dry_run = 1;
 }
 if (force_dry) {
 g_dry_run = 1;
 printf("[bridge] force dry-run\n");
 }

 signal(SIGINT, on_sig);
 signal(SIGTERM, on_sig);
 signal(SIGPIPE, SIG_IGN);

 listen_fd = socket(AF_INET, SOCK_STREAM, 0);
 if (listen_fd < 0) {
 perror("socket");
 return 1;
 }
 setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));
 memset(&addr, 0, sizeof(addr));
 addr.sin_family = AF_INET;
 addr.sin_addr.s_addr = htonl(INADDR_ANY);
 addr.sin_port = htons(g_listen_port);
 if (bind(listen_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
 perror("bind");
 close(listen_fd);
 return 1;
 }
 if (listen(listen_fd, 4) < 0) {
 perror("listen");
 close(listen_fd);
 return 1;
 }
 printf("[bridge] listening 0.0.0.0:%u , wait collector -p 127.0.0.1:%u\n",
 g_listen_port, g_listen_port);

 memset(&st, 0, sizeof(st));

 while (g_run) {
 pfd[0].fd = listen_fd;
 pfd[0].events = POLLIN;
 pfd[0].revents = 0;
 pfd[1].fd = conn_fd;
 pfd[1].events = POLLIN;
 pfd[1].revents = 0;

 if (poll(pfd, (conn_fd >= 0) ? 2 : 1, 500) < 0) {
 if (errno == EINTR) continue;
 perror("poll");
 break;
 }

 /* 新连接（监管平台：collector 重连时替换旧连接） */
 if (pfd[0].revents & POLLIN) {
 int nfd = accept(listen_fd, NULL, NULL);
 if (nfd >= 0) {
 if (conn_fd >= 0) {
 close(conn_fd);//替换旧连接
 }
 conn_fd = nfd;
 memset(&st, 0, sizeof(st));
 printf("[bridge] collector connected fd=%d\n", conn_fd);
 }
 }

 /* 读数据 */
 if (conn_fd >= 0 && (pfd[1].revents & (POLLIN | POLLHUP | POLLERR))) {
 uint8_t buf[1024];
 ssize_t n = read(conn_fd, buf, sizeof(buf));
 if (n > 0) {
 stream_feed(&st, buf, (size_t)n);
 } else if (n == 0 || (n < 0 && errno != EINTR && errno != EAGAIN)) {
 printf("[bridge] collector disconnected\n");
 close(conn_fd);
 conn_fd = -1;
 }
 }
 }

 if (conn_fd >= 0) close(conn_fd);
 close(listen_fd);
 printf("[bridge] exit frames=%llu heart=%llu upload_ok=%llu fail=%llu decode_fail=%llu\n",
 (unsigned long long)g_frames,
 (unsigned long long)g_hearts,
 (unsigned long long)g_upload_ok,
 (unsigned long long)g_upload_fail,
 (unsigned long long)g_decode_fail);
 return 0;
}
