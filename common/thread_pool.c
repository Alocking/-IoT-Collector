/* ============================================================================
 *  thread_pool.c —— 动态线程池
 *
 *  模块划分：
 *    模块 1：常量与静态变量
 *    模块 2：内部数据结构（tp_task_t / worker_arg_t）
 *    模块 3：任务打包/解包（借 env_frame_t 传指针）
 *    模块 4：worker 链表操作（spawn / unlink）
 *    模块 5：水位回调默认实现（投放 POOL_CMD）
 *    模块 6：worker 主循环（核心）
 *    模块 7：初始化 / 提交 / 销毁（对外接口 tp_init/tp_submit/tp_destroy）
 *
 *  并发要点：
 *    · tp->lock      保护 workers 链表、worker_num、shutdown
 *    · task_queue->lock  保护 task_queue 的环形缓冲区
 *    · 调用回调时必须在锁外，避免 AB-BA 死锁
 *    · 所有 locked 后缀的函数要求调用者持有对应锁
 *    · 编译必须 -pthread
 * ============================================================================
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "iot_collector.h"

/* ============================================================================
 *  模块 1：常量与静态变量
 * ============================================================================
 */
#define IDLE_TIMEOUT_SEC 60    /* 空闲 >60s 触发缩容 */
#define WATER_HIGH_THRESHOLD 3 /* 水位 >80% 持续 3 个采样周期 */

/* PDF 5.2 特殊控制任务（投放到队列中的"命令帧"）
 * 通过 env_frame_t.station_id 字段传递：
 *   station_id == POOL_CMD_SCALE_UP   → 扩容
 *   station_id == POOL_CMD_SCALE_DOWN → 缩容
 */
#define POOL_CMD_SCALE_UP 1
#define POOL_CMD_SCALE_DOWN 2

/* PDF 5.1 结构没有 min_workers / water_high_count 字段，
 * PDF 5.2 又需要这两个值，用文件静态变量存储（本项目单实例）。
 *
 * 为什么不放结构体？
 *   → PDF 5.1 明文规定 thread_pool_t 只有 9 个字段，不得增删。
 *
 * 单实例限制：
 *   → 同一进程只能创建一个线程池，否则两者会互相覆盖这两个值。
 *   → 本项目 IoT-Collector 单进程单线程池，符合。
 */
static uint32_t g_min_workers = 2;
static uint32_t g_water_high_count = 0;

/* ============================================================================
 *  模块 2：内部数据结构
 * ============================================================================
 */

/* 任务负载：真正的任务数据
 * 通过 env_frame_t 的 payload 字段传指针（详见模块 3 注释）
 */
typedef struct tp_task
{
    tp_task_fn fn;
    void *arg;
} tp_task_t;

/* worker 线程参数（pthread_create 时传递）
 * 生命周期：tp_worker 入口处 free，之后不再使用
 */
typedef struct
{
    thread_pool_t *tp;
    worker_t *self;
} worker_arg_t;

/* 前向声明：避免函数定义顺序问题
 *   tp_worker 被 tp_spawn_locked 引用，但定义在后面
 *   tp_spawn_locked / tp_unlink_locked 被 tp_worker 引用
 *   → 三者互相引用，需要提前声明
 */
static void *tp_worker(void *arg);
static int tp_spawn_locked(thread_pool_t *tp);
static void tp_unlink_locked(thread_pool_t *tp, worker_t *self);

/* ============================================================================
 *  模块 3：任务打包/解包
 *
 *  【核心难点】
 *  PDF 5.1 要求 task_queue 类型为 ring_queue_t*，其 buf 元素类型是
 *  env_frame_t（32 字节）。但线程池实际需要传递的是 tp_task_t* 指针。
 *
 *  【解决方案】
 *  借用 env_frame_t 的 payload 字段传指针：
 *    · frame_type 字段：标记帧类型（任务/命令）
 *    · station_id 字段：存 POOL_CMD 命令码（控制命令时）
 *    · timestamp_sec + temperature + humidity 共 8 字节：存 64 位指针
 *
 *  env_frame_t 字段使用表：
 *    +------------------+--------+--------------------------+
 *    | 字段             | 大小   | 用途                     |
 *    +------------------+--------+--------------------------+
 *    | magic[2]         | 2      | 不用                     |
 *    | version          | 1      | 不用                     |
 *    | frame_type       | 1      | ★ 任务类型标记           |
 *    | station_id       | 2      | ★ POOL_CMD 命令码        |
 *    | timestamp_sec    | 4      | ★←──┐                    |
 *    | temperature      | 2      | ★   │ 合并 8 字节存指针  |
 *    | humidity         | 2      | ★←──┘                    |
 *    | pm25 ... noise   | 8      | 不用                     |
 *    | crc16            | 2      | 不用                     |
 *    +------------------+--------+--------------------------+
 * ============================================================================
 */

/* 打包：把 tp_task_t* 指针塞进 env_frame_t 的 8 字节 payload
 *   前置条件：ef 有效，t 可为 NULL（控制命令时不需要任务负载）
 *   输出：ef 被填充为 FRAME_DATA 类型，payload 存 t 指针
 */
static void tp_pack_task(env_frame_t *ef, tp_task_t *t)
{
    memset(ef, 0, sizeof(*ef));
    ef->frame_type = FRAME_DATA;
    memcpy(&ef->timestamp_sec, &t, sizeof(t));
}

/* 解包：从 env_frame_t 的 payload 中取回 tp_task_t* 指针
 *   返回：任务的 tp_task_t*（可能为 NULL，此时调用方应跳过执行）
 */
static tp_task_t *tp_unpack_task(const env_frame_t *ef)
{
    tp_task_t *t = NULL;
    memcpy(&t, &ef->timestamp_sec, sizeof(t));
    return t;
}

/* ============================================================================
 *  模块 4：worker 链表操作（调用前必须持有 tp->lock）
 * ============================================================================
 */

/* ----------------------------------------------------------------------------
 *  tp_spawn_locked —— 创建一个新的 worker 线程并加入链表
 *
 *  前置条件：调用者必须持有 tp->lock
 *
 *  参数：tp  线程池对象
 *
 *  返回：0 成功；-1 失败（到达 max_workers 或分配失败）
 *
 *  步骤：
 *    1. 检查上限（worker_num >= max_workers 则拒绝）
 *    2. 分配 worker_t 节点
 *    3. 分配 worker_arg_t 参数
 *    4. pthread_create 创建线程
 *    5. pthread_detach 分离线程（否则需 join，导致资源泄漏）
 *    6. 头插法加入 workers 链表
 *    7. worker_num++
 * ---------------------------------------------------------------------------- */
static int tp_spawn_locked(thread_pool_t *tp)
{
    /* 步骤 1：上限检查（PDF 5.2：不超过 max_workers） */
    if (tp->worker_num >= tp->max_workers) /* PDF 5.2：上限 max_workers */
        return -1;

    /* 步骤 2：分配 worker 节点 */
    worker_t *w = (worker_t *)calloc(1, sizeof(worker_t));
    if (!w)
        return -1;
    w->active = 1;
    /* 步骤 3：分配线程参数 */
    worker_arg_t *wa = (worker_arg_t *)malloc(sizeof(worker_arg_t));
    if (!wa)
    {
        free(w);
        return -1;
    }
    wa->tp = tp;
    wa->self = w;
    /* 步骤 4：创建线程，入口函数为 tp_worker */
    if (pthread_create(&w->tid, NULL, tp_worker, wa) != 0)
    {
        free(w);
        free(wa);
        return -1;
    }

    /* 步骤 5：分离线程
     *   理由：worker 会动态创建/销毁，无法预知何时退出
     *        如果不 detach，则线程退出后资源不回收（需要 join）
     *        detach 后线程结束时内核自动回收资源
     */
    pthread_detach(w->tid);
    /* 步骤 6：头插法加入链表（O(1) 插入，不需要遍历） */
    w->next = tp->workers;
    tp->workers = w;
    /* 步骤 7：更新计数 */
    tp->worker_num++;
    return 0;
}
/* ----------------------------------------------------------------------------
 *  tp_unlink_locked —— 从链表中摘除一个 worker
 *
 *  前置条件：调用者必须持有 tp->lock
 *
 *  参数：tp    线程池对象
 *        self  要摘除的 worker 节点
 *
 *  步骤：
 *    1. 遍历链表找到 self 的前驱节点
 *    2. 从链表摘除
 *    3. free(self)
 *    4. worker_num--
 *    5. 广播 tp->cond（通知可能在等 worker_num==0 的 tp_destroy）
 * ---------------------------------------------------------------------------- */
static void tp_unlink_locked(thread_pool_t *tp, worker_t *self)
{
    /* 步骤 1：找到 self 的前驱节点
     *   使用二级指针 pp 简化删除逻辑：
     *     pp 指向"指向当前节点的指针"
     *     初始 pp = &tp->workers
     *     每次 pp = &(*pp)->next
     *   找到后 *pp == self，直接 *pp = self->next 即可摘除
     */
    worker_t **pp = &tp->workers;
    while (*pp && *pp != self)
        pp = &(*pp)->next;

    /* 步骤 2 & 3：从链表摘除并释放 */
    if (*pp)
        *pp = self->next;
    free(self);

    /* 步骤 4：更新计数 */
    tp->worker_num--;

    /* 步骤 5：广播通知（可能 tp_destroy 正在等待 worker_num==0） */
    pthread_cond_broadcast(&tp->cond);
}

/* ============================================================================
 *  模块 5：水位回调默认实现
 *
 *  PDF 5.1：thread_pool_t 中有 on_high_water / on_low_water 两个回调字段
 *  PDF 5.2：扩容/缩容均通过向队列投放特殊控制任务实现
 *
 *  本模块提供两个默认回调：
 *    · on_high_water  → 投放 POOL_CMD_SCALE_UP 到 task_queue
 *    · on_low_water   → 投放 POOL_CMD_SCALE_DOWN 到 task_queue
 *
 *  为什么用 FRAME_ALARM 类型？
 *    · ring_push 对 FRAME_ALARM 有特殊处理：队列满时覆盖最旧普通帧
 *    · 保证控制命令一定能进队列（否则 tp_submit 会阻塞）
 *    · 代价：可能覆盖 1 个 DATA 任务，但这是 PDF 4.2 定义的预期行为
 * ============================================================================
 */

/* ----------------------------------------------------------------------------
 *  tp_default_on_high_water —— 高水位回调（扩容）
 *
 *  由 tp_submit 在水位持续 >80% 时调用
 *  行为：投放一个 SCALE_UP 命令到 task_queue
 *        worker 会在取任务时识别到命令并 spawn 新 worker
 * ---------------------------------------------------------------------------- */
static void tp_default_on_high_water(thread_pool_t *tp)
{
    env_frame_t ef;
    memset(&ef, 0, sizeof(ef));
    ef.frame_type = FRAME_ALARM;      /* 高优先级：可覆盖最旧普通任务 */
    ef.station_id = POOL_CMD_SCALE_UP;
    ring_push(tp->task_queue, &ef);
}

/* ----------------------------------------------------------------------------
 *  tp_default_on_low_water —— 低水位回调（缩容）
 *
 *  由 worker 空闲 60s 后自行调用
 *  行为：投放一个 SCALE_DOWN 命令到 task_queue
 *        worker 取到后判断 worker_num > min_workers 则自己退出
 * ---------------------------------------------------------------------------- */
static void tp_default_on_low_water(thread_pool_t *tp)
{
    env_frame_t ef;
    memset(&ef, 0, sizeof(ef));
    ef.frame_type = FRAME_ALARM;      /* 同样高优先级 */
    ef.station_id = POOL_CMD_SCALE_DOWN;
    ring_push(tp->task_queue, &ef);
}

/* ============================================================================
 *  模块 6：worker 主循环（核心）
 *
 *  每个 worker 线程执行此函数，直到收到退出信号
 *
 *  主循环流程：
 *    ┌───────────────────────────────────┐
 *    │  1. 检查 shutdown（每轮）         │
 *    │  2. 从 task_queue 取任务（1s超时）│
 *    │  3. 若超时：累计空闲时长，触发缩容│
 *    │  4. 若取到：按 frame_type 分派    │
 *    │     · FRAME_DATA  → 执行任务      │
 *    │     · FRAME_ALARM → 读取 POOL_CMD │
 *    └───────────────────────────────────┘
 * ============================================================================
 */
static void *tp_worker(void *arg)
{
    worker_arg_t *wa = (worker_arg_t *)arg;
    thread_pool_t *tp = wa->tp;
    worker_t *self = wa->self;
    free(wa);

    time_t last_active = time(NULL);

    while (1)
    {
        /* 检查 shutdown */
        pthread_mutex_lock(&tp->lock);
        int sd = tp->shutdown;
        pthread_mutex_unlock(&tp->lock);
        if (sd)
            break;

        /* 带 1 秒超时地从 task_queue 取一帧 */
        env_frame_t ef;
        int got = 0;

        pthread_mutex_lock(&tp->task_queue->lock);
        if (tp->task_queue->count > 0)
        {
            ef = tp->task_queue->buf[tp->task_queue->tail];
            tp->task_queue->tail = (tp->task_queue->tail + 1) &
                                   (tp->task_queue->capacity - 1);
            tp->task_queue->count--;
            pthread_cond_broadcast(&tp->task_queue->not_full);
            got = 1;
            pthread_mutex_unlock(&tp->task_queue->lock);
        }
        else
        {
            struct timespec ts;
            clock_gettime(CLOCK_REALTIME, &ts);
            ts.tv_sec += 1;
            pthread_cond_timedwait(&tp->task_queue->not_empty,
                                   &tp->task_queue->lock, &ts);
            pthread_mutex_unlock(&tp->task_queue->lock);
        }

        if (!got)
        {
            /* 空闲 60s + worker 数 > min_workers：触发缩容（PDF 5.2） */
            if (time(NULL) - last_active >= IDLE_TIMEOUT_SEC)
            {
                pthread_mutex_lock(&tp->lock);
                int over_min = (tp->worker_num > g_min_workers);
                pthread_mutex_unlock(&tp->lock);
                if (over_min && tp->on_low_water)
                    tp->on_low_water(tp); /* 投放 POOL_CMD_SCALE_DOWN */
                last_active = time(NULL);
            }
            continue;
        }

        last_active = time(NULL);

        /* PDF 5.2：worker 自行感知并执行 */
        if (ef.frame_type == FRAME_DATA)
        {
            /* 普通任务：执行 */
            tp_task_t *t = tp_unpack_task(&ef);
            if (t)
            {
                if (t->fn)
                    t->fn(t->arg);
                free(t);
            }
            self->task_count++; /* PDF 5.1：task_count++ */
        }
        else if (ef.frame_type == FRAME_ALARM)
        {
            /* 控制命令 */
            uint8_t cmd = (uint8_t)ef.station_id;
            if (cmd == POOL_CMD_SCALE_UP)
            {
                pthread_mutex_lock(&tp->lock);

                tp_spawn_locked(tp); /* worker 自行扩 */
                pthread_mutex_unlock(&tp->lock);
            }
            else if (cmd == POOL_CMD_SCALE_DOWN)
            {
                pthread_mutex_lock(&tp->lock);
                if (tp->worker_num > g_min_workers)
                {
                    tp_unlink_locked(tp, self); /* worker 自行缩（保底 min_workers） */
                    pthread_mutex_unlock(&tp->lock);
                    return NULL;
                }
                pthread_mutex_unlock(&tp->lock);
            }
        }
    }

    /* shutdown 退出 */
    pthread_mutex_lock(&tp->lock);
    tp_unlink_locked(tp, self);
    pthread_mutex_unlock(&tp->lock);
    return NULL;
}

/* ============================================================================
 *  模块 7：对外接口（初始化 / 提交 / 销毁）
 * ============================================================================
 */

/* ----------------------------------------------------------------------------
 *  tp_init —— 初始化线程池
 *
 *  参数：
 *    tp     线程池对象指针
 *    min_w  最小线程数（保底）
 *    max_w  最大线程数（上限）
 *    qcap   任务队列容量（必须是 2 的幂）
 *
 *  返回：0 成功，-1 失败
 *
 *  步骤：
 *    1. 参数校验
 *    2. 清零结构体 + 设置回调
 *    3. 初始化锁和条件变量
 *    4. 分配任务队列（调 ring_init）
 *    5. 预启动 min_w 个 worker
 * ---------------------------------------------------------------------------- */
int tp_init(thread_pool_t *tp, uint32_t min_w, uint32_t max_w, uint32_t qcap)
{
    if (min_w == 0 || max_w < min_w || qcap == 0)
        return -1;
    if (qcap & (qcap - 1))
        return -1; /* 容量必须 2 的幂 */

    memset(tp, 0, sizeof(*tp));
    tp->max_workers = max_w;
    tp->on_high_water = tp_default_on_high_water;
    tp->on_low_water = tp_default_on_low_water;

    g_min_workers = min_w;
    g_water_high_count = 0;

    pthread_mutex_init(&tp->lock, NULL);
    pthread_cond_init(&tp->cond, NULL);

    /* PDF 5.1：复用环形队列作为任务队列 */
    tp->task_queue = (ring_queue_t *)calloc(1, sizeof(ring_queue_t));
    if (!tp->task_queue)
        return -1;
    if (ring_init(tp->task_queue, qcap) != 0)
    {
        free(tp->task_queue);
        tp->task_queue = NULL;
        return -1;
    }

    /* 预启动 min_workers */
    pthread_mutex_lock(&tp->lock);
    for (uint32_t i = 0; i < min_w; ++i)
    {
        if (tp_spawn_locked(tp) != 0)
        {
            if (tp->worker_num == 0)
            {
                pthread_mutex_unlock(&tp->lock);
                ring_destroy(tp->task_queue);
                free(tp->task_queue);
                tp->task_queue = NULL;
                pthread_mutex_destroy(&tp->lock);
                pthread_cond_destroy(&tp->cond);
                return -1;
            }
            break;
        }
    }
    pthread_mutex_unlock(&tp->lock);
    return 0;
}

/* ----------------------------------------------------------------------------
 *  tp_submit —— 提交任务到线程池
 *
 *  参数：
 *    tp   线程池对象
 *    fn   任务函数
 *    arg  任务参数
 *
 *  返回：0 成功，-1 失败
 *
 *  步骤：
 *    1. 打包任务（tp_task_t → env_frame_t）
 *    2. ring_push 入队（队列满时阻塞等待）
 *    3. 读水位 = count / capacity
 *    4. 水位 > 80% 连续 3 次 → 触发 on_high_water 回调（扩容）
 * ---------------------------------------------------------------------------- */
int tp_submit(thread_pool_t *tp, tp_task_fn fn, void *arg)
{
    if (!fn)
        return -1;

    tp_task_t *t = (tp_task_t *)calloc(1, sizeof(tp_task_t));
    if (!t)
        return -1;
    t->fn = fn;
    t->arg = arg;

    env_frame_t ef;
    tp_pack_task(&ef, t);

    /* 1. 入队 */
    if (ring_push(tp->task_queue, &ef) != 0)
    {
        free(t);
        return -1;
    }

    /* 2. 读水位（PDF 5.2：队列水位 = count / capacity） */
    pthread_mutex_lock(&tp->task_queue->lock);
    uint32_t cap = tp->task_queue->capacity;
    uint32_t len = tp->task_queue->count;
    pthread_mutex_unlock(&tp->task_queue->lock);

    uint32_t water = (uint32_t)((uint64_t)len * 100 / cap);

    /* 3. PDF 5.2：水位 >80% 持续 3 个采样周期 → 触发扩容 */
    int trigger = 0;
    pthread_mutex_lock(&tp->lock);
    if (water > 80)
    {
        g_water_high_count++;
        if (g_water_high_count >= WATER_HIGH_THRESHOLD)// 3 个采样周期
        {
            g_water_high_count = 0;
            trigger = 1;
        }
    }
    else
    {
        g_water_high_count = 0;
    }
    pthread_mutex_unlock(&tp->lock);

    /* 4. 锁外调用回调，避免死锁 */
    if (trigger && tp->on_high_water)
    {
        tp->on_high_water(tp);
    } /* 投放 POOL_CMD_SCALE_UP */

    return 0;
}

/* ----------------------------------------------------------------------------
 *  tp_destroy —— 销毁线程池（优雅退出）
 *
 *  参数：tp  线程池对象
 *
 *  退出序列（PDF 第八章"严格逆序"）：
 *    1. 置 shutdown = 1
 *    2. 唤醒所有阻塞在 task_queue 上的 worker
 *    3. 等所有 worker 自行退出（worker_num == 0）
 *    4. 清理队列中未执行的任务
 *    5. 释放所有资源（队列、锁、条件变量）
 *
 *  说明：不使用 pthread_cancel（PDF 5.2 明文要求）
 *       每个 worker 自行检查 shutdown 后退出，保证任务不被半途中断
 * ---------------------------------------------------------------------------- */

void tp_destroy(thread_pool_t *tp)
{
    pthread_mutex_lock(&tp->lock);
    tp->shutdown = 1;
    pthread_mutex_unlock(&tp->lock);

    ring_wake_all(tp->task_queue); /* 唤醒所有阻塞的 worker */

    /* PDF 第八章：等待所有 worker 自行退出 */
    pthread_mutex_lock(&tp->lock);
    while (tp->worker_num > 0)
        pthread_cond_wait(&tp->cond, &tp->lock);
    pthread_mutex_unlock(&tp->lock);

    /* 清理队列里未执行的任务 */
    pthread_mutex_lock(&tp->task_queue->lock);
    while (tp->task_queue->count > 0)
    {
        env_frame_t ef = tp->task_queue->buf[tp->task_queue->tail];
        tp->task_queue->tail = (tp->task_queue->tail + 1) &
                               (tp->task_queue->capacity - 1);
        tp->task_queue->count--;
        if (ef.frame_type == FRAME_DATA)
        {
            tp_task_t *t = tp_unpack_task(&ef);
            free(t);
        }
    }
    pthread_mutex_unlock(&tp->task_queue->lock);

    ring_destroy(tp->task_queue);
    free(tp->task_queue);
    tp->task_queue = NULL;

    pthread_mutex_destroy(&tp->lock);
    pthread_cond_destroy(&tp->cond);
}
