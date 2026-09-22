//断链缓存 — 五要点
//  1) 文件 + 读写游标：append 顺序写，replay 按游标补传，无重复无丢失
//  2) 游标落盘 path.cursor：进程崩溃重启后仍能接着补
//  3) 多分发线程并发 append，内部 cache_lock 互斥
//  4) 补传帧打 FRAME_RETRY，走 g_retry_bucket 独立限速，不挤占实时上报
//  5) fsync 刷盘：标准IO fclose 后不可再 fsync，须在关闭前完成
#include "iot_collector.h"//引入项目总头文件
#include <sys/stat.h>
#include <fcntl.h>//open/lseek

static int cache_fd = -1;//缓存文件描述符
static off_t write_cursor = 0;//写游标（字节偏移）
static off_t read_cursor = 0;//读游标（已补传位置）
static pthread_mutex_t cache_lock = PTHREAD_MUTEX_INITIALIZER;//保护游标与 fd

#define CURSOR_FILE_SUFFIX ".cursor"//游标文件后缀

//加载读游标：从 path.cursor 读上次补传位置
static int load_cursor(const char *path)
{
    char cursor_path[256];//游标文件路径
    FILE *f = NULL;//标准IO 文件指针
    snprintf(cursor_path, sizeof(cursor_path), "%s%s", path, CURSOR_FILE_SUFFIX);
    f = fopen(cursor_path, "rb");//打开游标文件
    if (f == NULL) {//首次运行无游标
        read_cursor = 0;//从头补
        return IOT_OK;
    }
    if (fread(&read_cursor, sizeof(off_t), 1, f) != 1) {//读失败
        read_cursor = 0;//退回 0
    }
    fclose(f);//关闭
    return IOT_OK;
}

//保存读游标到磁盘
static int save_cursor(const char *path)
{
    char cursor_path[256];//路径缓冲
    FILE *f = NULL;//文件指针
    snprintf(cursor_path, sizeof(cursor_path), "%s%s", path, CURSOR_FILE_SUFFIX);
    f = fopen(cursor_path, "wb");//写打开
    if (f == NULL) {
        return IOT_ERR_IO;//失败
    }
    if (fwrite(&read_cursor, sizeof(off_t), 1, f) != 1) {//写出游标
        fclose(f);
        return IOT_ERR_IO;
    }
    fflush(f);//冲用户态缓冲
    fsync(fileno(f));//落盘：必须在 fclose 之前
    fclose(f);//再关闭
    return IOT_OK;
}

//打开断链缓存文件（带游标）
int cache_writer_open(const char *path)
{
    if (path == NULL) {//判空
        return IOT_ERR_GENERIC;
    }
    pthread_mutex_lock(&cache_lock);//加锁
    cache_fd = open(path, O_RDWR | O_CREAT, 0644);//系统调用 open
    if (cache_fd < 0) {//打开失败
        pthread_mutex_unlock(&cache_lock);
        return IOT_ERR_IO;
    }
    write_cursor = lseek(cache_fd, 0, SEEK_END);//写游标定位到文件末尾
    load_cursor(path);//读上次补传位置
    pthread_mutex_unlock(&cache_lock);
    return IOT_OK;
}

//追加一帧到本地缓存（多线程并发安全）
int cache_writer_append(const env_frame_t *frame)
{
    ssize_t w = 0;//write 返回值
    if (frame == NULL) {//判空
        return IOT_ERR_GENERIC;
    }
    pthread_mutex_lock(&cache_lock);//临界区
    if (cache_fd < 0) {//未打开
        pthread_mutex_unlock(&cache_lock);
        return IOT_ERR_IO;
    }
    w = write(cache_fd, frame, sizeof(env_frame_t));//整帧写入
    if (w != (ssize_t)sizeof(env_frame_t)) {//短写/失败
        pthread_mutex_unlock(&cache_lock);
        return IOT_ERR_IO;
    }
    write_cursor += sizeof(env_frame_t);//写游标前进
    pthread_mutex_unlock(&cache_lock);
    return IOT_OK;
}

//将缓存刷盘 fsync
int cache_writer_flush(void)
{
    int ret = 0;
    pthread_mutex_lock(&cache_lock);
    if (cache_fd < 0) {
        pthread_mutex_unlock(&cache_lock);
        return IOT_ERR_IO;
    }
    ret = fsync(cache_fd);//内核缓冲 → 磁盘
    pthread_mutex_unlock(&cache_lock);
    return (ret == 0) ? IOT_OK : IOT_ERR_IO;
}

//关闭缓存文件
int cache_writer_close(void)
{
    pthread_mutex_lock(&cache_lock);
    if (cache_fd >= 0) {
        close(cache_fd);//关闭 fd
        cache_fd = -1;//置非法
    }
    pthread_mutex_unlock(&cache_lock);
    return IOT_OK;
}

//恢复后按序补传：读游标 → 写游标，失败停在原地下次续传
int cache_replay_start(int fd)
{
    env_frame_t frm;//读出的帧
    ssize_t r = 0;//read 返回值
    int send_ret = 0;//发送结果

    pthread_mutex_lock(&cache_lock);
    if (cache_fd < 0 || read_cursor >= write_cursor) {//无积压
        pthread_mutex_unlock(&cache_lock);
        return IOT_OK;
    }
    lseek(cache_fd, read_cursor, SEEK_SET);//定位到读游标
    while (read_cursor < write_cursor) {//还有未补传数据
        r = read(cache_fd, &frm, sizeof(env_frame_t));//读一帧
        if (r != (ssize_t)sizeof(env_frame_t)) {
            break;//文件损坏/截断，停
        }
        frm.frame_type = FRAME_RETRY;//打补传标记
        send_ret = unicast_sender_send_with_bucket(fd, &frm, sizeof(frm), g_retry_bucket);
        if (send_ret != IOT_OK) {//发送失败：游标不动
            break;//下次重试
        }
        read_cursor += sizeof(env_frame_t);//成功才推进游标
        save_cursor("./cache.dat");//落盘游标
    }
    pthread_mutex_unlock(&cache_lock);
    return IOT_OK;
}

//返回尚未补传的帧数（缓存积压量）
uint64_t cache_pending_frames(void)
{
    uint64_t pending = 0;//积压帧数
    pthread_mutex_lock(&cache_lock);
    if (write_cursor > read_cursor) {
        pending = (uint64_t)(write_cursor - read_cursor) / sizeof(env_frame_t);
    }
    pthread_mutex_unlock(&cache_lock);
    return pending;
}
