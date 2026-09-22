/* ============================================================================
 *  ring_queue.c —— 有界环形队列
 *
 *  模块划分：
 *    模块 1：初始化 / 销毁（ring_init / ring_destroy）
 *    模块 2：内部辅助函数（drop_oldest_normal）
 *    模块 3：入队（ring_push 阻塞 / ring_push_try 非阻塞）
 *    模块 4：批量出队（ring_pop_batch）
 *    模块 5：唤醒 / 水位（ring_wake_all / ring_count）
 *
 *  并发要点：
 *    · lock       保护 buf / head / tail / count / dropped
 *    · not_empty  生产者 signal，消费者 wait
 *    · not_full   消费者 broadcast，生产者 wait
 *    · 位运算取模 (x+1) & (cap-1) 避免 % 除法
 *
 *  关键设计决策：
 *    1. 容量取 2 的幂：位运算代替取模
 *    2. not_full 用 broadcast / not_empty 用 signal
 *    3. 告警帧覆盖最旧普通帧（保告警不丢）
 *    4. 接入层必须 ring_push_try，避免被队列卡住导致 socket 溢出
 * ============================================================================
 */
#include "iot_collector.h"
#include <stdlib.h>
#include <string.h>

/* ============================================================================
 *  模块 1：初始化 / 销毁
 * ============================================================================
 */

/* ----------------------------------------------------------------------------
 *  ring_init —— 初始化环形队列
 *
 *  参数：
 *    q    队列对象
 *    cap  容量（必须是 2 的幂）
 *
 *  返回：0 成功，-1 失败
 *
 *  步骤：
 *    1. 校验 cap：必须非 0 且是 2 的幂
 *    2. 分配 cap 个 env_frame_t 的内存
 *    3. 初始化指针、计数、统计
 *    4. 初始化互斥锁 + 两个条件变量
 * ---------------------------------------------------------------------------- */
int ring_init(ring_queue_t *q, uint32_t cap)
{
    /* -------- 步骤 1：容量校验 --------
     * PDF 决策 1：容量取 2 的幂，位运算取模
     *
     * 判断 2 的幂：(cap & (cap - 1)) == 0
     *   cap = 8  →  8 & 7 = 0   ✓
     *   cap = 7  →  7 & 6 = 6   ✗
     *   cap = 16 →  16 & 15 = 0 ✓
     *   cap = 15 →  15 & 14 = 14 ✗
     */
    if (cap == 0 || (cap & (cap - 1)) != 0)
        return -1;

    /* -------- 步骤 2：分配内存 -------- */
    q->buf = (env_frame_t *)calloc(cap, sizeof(env_frame_t));
    if (!q->buf) return -1;

    /* -------- 步骤 3：初始化字段 -------- */
    q->capacity = cap;
    q->head = q->tail = q->count = q->dropped = 0;

    /* -------- 步骤 4：初始化同步原语 -------- */
    pthread_mutex_init(&q->lock, NULL);
    pthread_cond_init(&q->not_empty, NULL);
    pthread_cond_init(&q->not_full, NULL);
    return 0;
}

/* ----------------------------------------------------------------------------
 *  ring_destroy —— 销毁环形队列
 *
 *  参数：q  队列对象
 *
 *  步骤：
 *    1. 销毁互斥锁
 *    2. 销毁两个条件变量
 *    3. 释放 buf 内存
 *
 *  注意：调用前应保证没有线程正在使用此队列
 * ---------------------------------------------------------------------------- */
void ring_destroy(ring_queue_t *q)
{
    pthread_mutex_destroy(&q->lock);
    pthread_cond_destroy(&q->not_empty);
    pthread_cond_destroy(&q->not_full);
    free(q->buf);
    q->buf = NULL;   /* 防止悬空指针 */
}

/* ============================================================================
 *  模块 2：内部辅助函数
 * ============================================================================
 */

/* ----------------------------------------------------------------------------
 *  drop_oldest_normal —— 丢弃队内最旧的普通帧
 *
 *  前置条件：调用者必须持有 q->lock
 *
 *  参数：q  队列对象
 *
 *  用途：PDF 4.2 告警帧覆盖策略的核心
 *        当队列满且要插入告警帧时，需要腾出一个位置
 *        覆盖策略：找最旧的普通帧删掉（FIFO 语义）
 *
 *  算法：
 *    1. 从 tail 开始扫描，找到第一个 frame_type != FRAME_ALARM 的帧
 *    2. 把它删掉（后面的元素整体前移一位）
 *    3. head 回退一格，count--，dropped++
 *
 *  图示（容量 8，队列满）：
 *    覆盖前：
 *      [D0][D1][A99][D2][D3][D4][D5][D6]
 *       ▲                               ▲
 *      tail                            head
 *
 *    找到最旧普通帧 D0（下标 0）：
 *      删除 D0，后续左移：
 *      [D1][A99][D2][D3][D4][D5][D6][  ]
 *                                        ▲
 *                                      head（回退一格）
 *
 *  边界情况：若队列全是告警帧（找不到普通帧）
 *            此函数不做任何事，调用方需自行处理（比如继续等待）
 * ---------------------------------------------------------------------------- */
static void drop_oldest_normal(ring_queue_t *q)
{
    /* 步骤 1：从 tail 起扫描，找到第一个非告警帧 */
    for (uint32_t i = 0; i < q->count; ++i) {
        uint32_t idx = (q->tail + i) & (q->capacity - 1);   /* 环形下标 */
        if (q->buf[idx].frame_type != FRAME_ALARM) {
            /* 步骤 2：把 idx 之后的元素整体前移一位 */
            for (uint32_t j = i; j + 1 < q->count; ++j) {
                uint32_t cur  = (q->tail + j)     & (q->capacity - 1);
                uint32_t next = (q->tail + j + 1) & (q->capacity - 1);
                q->buf[cur] = q->buf[next];
            }
            /* 步骤 3：head 回退一格，count 减 1，dropped 加 1 */
            q->head = (q->head - 1) & (q->capacity - 1);
            q->count--;
            q->dropped++;
            return;   /* 覆盖完成 */
        }
    }
    /* 队列里全是告警帧：不做任何事，返回。
     * 调用方（ring_push）会继续走后续逻辑（尝试写入或等待）
     */
}

/* ============================================================================
 *  模块 3：入队（生产者接口）
 * ============================================================================
 */

/* ----------------------------------------------------------------------------
 *  ring_push —— 入队
 *
 *  参数：
 *    q      队列对象
 *    frame  要入队的帧（会被完整拷贝到 buf）
 *
 *  返回：0 成功，-1 失败（cond_wait 出错时）
 *
 *  满队策略（PDF 4.2 关键设计决策）：
 *    ┌────────────┬───────────────┬────────────────────────┐
 *    │ 新帧类型   │ 满队时行为     │ 理由                    │
 *    ├────────────┼───────────────┼────────────────────────┤
 *    │ FRAME_ALARM│ 覆盖最旧普通帧 │ 告警永不丢              │
 *    │ 其他       │ 阻塞等待       │ 保序，不丢数据          │
 *    └────────────┴───────────────┴────────────────────────┘
 *
 *  唤醒策略：
 *    push 后 signal(not_empty)，唤醒一个消费者
 *    （本项目多消费者，但 signal 也够用：一次只来一帧，唤醒一个就够）
 * ---------------------------------------------------------------------------- */
int ring_push(ring_queue_t *q, const env_frame_t *frame)
{
    pthread_mutex_lock(&q->lock);

    /* -------- 满队处理：循环直到有位置 -------- */
    while (q->count == q->capacity) {
        if (frame->frame_type == FRAME_ALARM) {
            /* 情况 A：告警帧 → 覆盖最旧普通帧，腾出位置 */
            drop_oldest_normal(q);
            break;   /* 已腾出位置（或全是告警帧），跳出 while 走写入逻辑 */
        }

        /* 情况 B：普通帧 → 等待消费者腾位（PDF 原文：等待或丢弃）
         * cond_wait 会释放 lock 并睡眠，被唤醒后重新持有 lock
         */
        if (pthread_cond_wait(&q->not_full, &q->lock) != 0) {
            pthread_mutex_unlock(&q->lock);
            return -1;
        }
    }

    /* -------- 写入队头 -------- */
    q->buf[q->head] = *frame;                                 /* 拷贝整帧 */
    q->head = (q->head + 1) & (q->capacity - 1);              /* 位运算取模 */
    q->count++;

    /* 唤醒一个消费者 */
    pthread_cond_signal(&q->not_empty);
    pthread_mutex_unlock(&q->lock);
    return 0;
}

/* ============================================================================
 *  模块 4：批量出队（消费者接口）
 * ============================================================================
 */

/* ----------------------------------------------------------------------------
 *  ring_pop_batch —— 批量出队
 *
 *  参数：
 *    q      队列对象
 *    out    输出数组（调用者提供）
 *    max_n  一次最多取多少个
 *
 *  返回：实际取出的个数（0 表示无数据或进程退出）
 *
 *  行为：
 *    · 队列空时阻塞等待生产者入队
 *    · 若 g_running == 0（进程正在退出），立即返回 0
 *    · 一次锁内取 min(count, max_n) 个，减少锁竞争
 *
 *  唤醒策略：
 *    出队后 broadcast(not_full)
 *    （一次腾出多个空位，需要唤醒所有等待的生产者，避免遗漏）
 *
 *  性能优势：
 *    取 1 个：lock/unlock 各 64 次（64 个元素）
 *    取 64 个：lock/unlock 各 1 次
 *    → 锁开销减少 64 倍
 * ---------------------------------------------------------------------------- */
uint32_t ring_pop_batch(ring_queue_t *q, env_frame_t *out, uint32_t max_n)
{
    pthread_mutex_lock(&q->lock);

    /* -------- 空队列等待 -------- */
    while (q->count == 0) {
        /* PDF 第八章退出序列：g_running=0 时立即返回，避免死锁 */
        if (!g_running) {
            pthread_mutex_unlock(&q->lock);
            return 0;
        }
        pthread_cond_wait(&q->not_empty, &q->lock);
    }

    /* -------- 批量出队 -------- */
    uint32_t n = q->count < max_n ? q->count : max_n;   /* min */
    for (uint32_t i = 0; i < n; i++) {
        out[i]  = q->buf[q->tail];                        /* 拷贝到输出 */
        q->tail = (q->tail + 1) & (q->capacity - 1);      /* tail 前进 */
    }
    q->count -= n;

    /* -------- 广播唤醒所有生产者 --------
     * 用 broadcast 而非 signal：
     *   一次取走 n 个元素，队列腾出 n 个空位
     *   若有多个生产者被阻塞，需全部唤醒才能填满空位
     */
    pthread_cond_broadcast(&q->not_full);

    pthread_mutex_unlock(&q->lock);
    return n;
}

/* ============================================================================
 *  模块 5：唤醒所有阻塞线程
 * ============================================================================
 */

/* ----------------------------------------------------------------------------
 *  ring_wake_all —— 唤醒所有阻塞线程
 *
 *  参数：q  队列对象
 *
 *  用途：PDF 第八章退出序列
 *        "唤醒所有阻塞线程（broadcast 条件变量）"
 *
 *  使用场景：
 *    1. tp_destroy 销毁线程池时
 *    2. main 优雅退出时
 *
 *  原理：
 *    · broadcast(not_empty)：唤醒所有等待数据的消费者
 *    · broadcast(not_full)：唤醒所有等待空位的生产者
 *    · 被唤醒的线程会重新检查条件（count、g_running、shutdown）
 *      并根据情况决定继续等待还是退出
 * ---------------------------------------------------------------------------- */
void ring_wake_all(ring_queue_t *q)
{
    pthread_mutex_lock(&q->lock);
    pthread_cond_broadcast(&q->not_empty);//唤醒等待数据的消费者
    pthread_cond_broadcast(&q->not_full);//唤醒等待空位的生产者
    pthread_mutex_unlock(&q->lock);
}

/* 兼容旧名：dispatcher/main 历史代码用过 ring_queue_wakeup_all */
void ring_queue_wakeup_all(ring_queue_t *q)
{
    ring_wake_all(q);//转调统一实现
}

/* ----------------------------------------------------------------------------
 *  ring_count —— 查看当前有效元素个数
 *
 *  用途：stats.c 每秒快照队列水位
 *  并发：读 count 前加锁，避免与生产/消费线程竞态
 * ---------------------------------------------------------------------------- */
uint32_t ring_count(ring_queue_t *q)
{
    uint32_t n = 0;
    if (q == NULL){//判空
        return 0;
    }
    pthread_mutex_lock(&q->lock);//加锁读
    n = q->count;//拷贝计数
    pthread_mutex_unlock(&q->lock);//解锁
    return n;//返回水位
}

/* ----------------------------------------------------------------------------
 *  ring_push_try —— 非阻塞入队（接入层 / 清洗层专用）
 *
 *  返回：IOT_OK 成功；IOT_ERR_FULL 队列满且无法挤占
 *
 *  满队策略：
 *    · FRAME_ALARM：挤掉最旧普通帧后写入（告警永不丢）
 *    · 其他帧：直接丢弃并 dropped++（接收线程绝不能被队列卡住）
 *
 *  为何必须 try 而不是阻塞：
 *    UDP 接入线程若在 ring_push 上睡眠，内核 socket 缓冲会溢出丢包；
 *    清洗线程若阻塞在 Q2，Q1 会迅速堆满。两处都用 try-push。
 * ---------------------------------------------------------------------------- */
int ring_push_try(ring_queue_t *q, const env_frame_t *frame)
{
    if (q == NULL || frame == NULL){//参数校验
        return IOT_ERR_GENERIC;//非法参数
    }

    pthread_mutex_lock(&q->lock);//进入临界区

    if (q->count == q->capacity) {//队列已满
        if (frame->frame_type == FRAME_ALARM) {//告警帧：挤占最旧普通帧
            drop_oldest_normal(q);
            if (q->count == q->capacity) {//全是告警帧，挤不动
                pthread_mutex_unlock(&q->lock);
                return IOT_ERR_FULL;//返回满
            }
        } else {//普通帧：直接丢弃
            q->dropped++;//统计丢帧
            pthread_mutex_unlock(&q->lock);
            return IOT_ERR_FULL;//返回满
        }
    }

    q->buf[q->head] = *frame;//整帧拷贝写入
    q->head = (q->head + 1) & (q->capacity - 1);//位运算取模前进写指针
    q->count++;//有效元素+1
    pthread_cond_signal(&q->not_empty);//唤醒一个消费者
    pthread_mutex_unlock(&q->lock);//离开临界区
    return IOT_OK;//成功
}
