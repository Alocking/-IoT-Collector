//令牌桶限速 — 五要点
//  1) cps=512KB/s，burst=1MB，tick=10ms：时钟线程 tbf_tick 补令牌
//  2) tbf_fetch_token：不足时 pthread_cond_wait，绝不能忙等
//  3) 单次请求超过 burst 时截断为 burst，否则永远凑不齐会死锁
//  4) 多桶实例：g_outbound_bucket 实时上报 / g_retry_bucket 补传，互不影响
//  5) throttled_send：先取令牌再 sendto；shutdown 时唤醒等待者优雅退出
#include "iot_collector.h"//引入项目总头文件

/* 进程内共享的出口令牌桶实例 */
tbf_t *g_outbound_bucket = NULL;   /* 单播上报限速 */
tbf_t *g_retry_bucket    = NULL;   /* 断链补传限速 */

int tbf_init(tbf_t **out, uint64_t cps, uint64_t burst)
{
    tbf_t *t = NULL;

    if (out == NULL || cps == 0u || burst == 0u) {
        return IOT_ERR_GENERIC;
    }

    t = (tbf_t *)calloc(1, sizeof(*t));
    if (t == NULL) {
        return IOT_ERR_NOMEM;
    }

    t->cps = cps;
    t->burst = burst;
    t->tokens = burst;          /* 初始满桶：允许启动瞬间的突发 */
    t->shutdown = 0;
    t->throttled_bytes = 0;

    if (pthread_mutex_init(&t->lock, NULL) != 0) {
        free(t);
        return IOT_ERR_GENERIC;
    }
    if (pthread_cond_init(&t->cond, NULL) != 0) {
        pthread_mutex_destroy(&t->lock);
        free(t);
        return IOT_ERR_GENERIC;
    }

    *out = t;
    return IOT_OK;
}

void tbf_destroy(tbf_t *tbf)
{
    if (tbf == NULL) {
        return;
    }
    pthread_cond_destroy(&tbf->cond);
    pthread_mutex_destroy(&tbf->lock);
    free(tbf);
}

void tbf_tick(tbf_t *tbf, uint64_t elapsed_ms)
{
    uint64_t add = 0;

    if (tbf == NULL || elapsed_ms == 0u) {
        return;
    }
    if (elapsed_ms > 60000u) {
        elapsed_ms = 60000u;    /* 时钟被长时间挂起时避免一次性灌爆，也防乘法溢出 */
    }

    add = (tbf->cps * elapsed_ms) / 1000u;

    pthread_mutex_lock(&tbf->lock);
    tbf->tokens += add;
    if (tbf->tokens > tbf->burst) {
        tbf->tokens = tbf->burst;
    }
    pthread_cond_broadcast(&tbf->cond);   /* 唤醒所有等令牌的发送线程重新竞争 */
    pthread_mutex_unlock(&tbf->lock);
}

int tbf_fetch_token(tbf_t *tbf, uint64_t want, int timeout_ms)
{
    int rc = 0;
    int counted = 0;

    if (tbf == NULL || want == 0u) {
        return IOT_ERR_GENERIC;
    }
    /* 单次请求不能超过桶容量，否则永远凑不齐会死等 */
    if (want > tbf->burst) {
        want = tbf->burst;
    }

    pthread_mutex_lock(&tbf->lock);

    for (;;) {
        if (tbf->shutdown != 0) {
            pthread_mutex_unlock(&tbf->lock);
            return IOT_ERR_SHUTDOWN;
        }
        if (tbf->tokens >= want) {
            tbf->tokens -= want;
            pthread_mutex_unlock(&tbf->lock);
            return IOT_OK;
        }

        /* 令牌不足：本次请求被限速，只统计一次 */
        if (counted == 0) {
            tbf->throttled_bytes += want;
            stats_on_throttled(want);
            counted = 1;
        }

        if (timeout_ms < 0) {
            rc = pthread_cond_wait(&tbf->cond, &tbf->lock);
        } else {
            struct timespec ts;
            clock_gettime(CLOCK_REALTIME, &ts);
            ts.tv_sec  += timeout_ms / 1000;
            ts.tv_nsec += (long)(timeout_ms % 1000) * 1000000L;
            if (ts.tv_nsec >= 1000000000L) {
                ts.tv_sec  += 1;
                ts.tv_nsec -= 1000000000L;
            }
            rc = pthread_cond_timedwait(&tbf->cond, &tbf->lock, &ts);
        }
        if (rc == ETIMEDOUT) {
            pthread_mutex_unlock(&tbf->lock);
            return IOT_ERR_TIMEOUT;
        }
    }
}

void tbf_shutdown(tbf_t *tbf)
{
    if (tbf == NULL) {
        return;
    }
    pthread_mutex_lock(&tbf->lock);
    tbf->shutdown = 1;
    pthread_cond_broadcast(&tbf->cond);
    pthread_mutex_unlock(&tbf->lock);
}

int throttled_send(int fd, const void *buf, size_t len,
                   const struct sockaddr_in *dest, tbf_t *bucket)
{
    const char *p = (const char *)buf;
    size_t remain = len;

    if (fd < 0 || buf == NULL || dest == NULL || bucket == NULL || len == 0u) {
        return IOT_ERR_GENERIC;
    }

    while (remain > 0u) {
        uint64_t want = (remain > TBF_MAX_FETCH_PER_OP) ? (uint64_t)TBF_MAX_FETCH_PER_OP
                                                        : (uint64_t)remain;
        ssize_t n = 0;
        int rc = tbf_fetch_token(bucket, want, -1);   /* 无限等待令牌 */

        if (rc == IOT_ERR_SHUTDOWN) {
            return IOT_ERR_SHUTDOWN;
        }
        if (rc < 0) {
            continue;    /* 其他异常：重新取令牌 */
        }

        /* UDP 为整包发送：sendto 要么全发要么失败 */
        n = sendto(fd, p, (size_t)want, 0,
                   (const struct sockaddr *)dest, sizeof(*dest));
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            return IOT_ERR_IO;
        }
        if ((size_t)n != (size_t)want) {
            return IOT_ERR_IO;
        }

        p += want;
        remain -= (size_t)want;
    }
    return IOT_OK;
}
