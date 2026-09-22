//数据清洗 — 五要点
//  1) 从 Q1 ring_pop_batch 批量取帧 → cleaner_process → ring_push_try 入 Q2
//  2) 物理量程校验：温度/湿度/PM/CO/噪声越界直接 CLEAN_DROP
//  3) 6 参数滑动窗口 3σ 突变检测（WINDOW_SIZE 满才检测）
//  4) 去重：时间戳倒退/重复 → CLEAN_DROP；告警帧特权直接 CLEAN_ALARM
//  5) 统计：stats_on_clean(0=pass/1=alarm/2=range/3=dup/4=reorder)
#include "iot_collector.h"//引入项目总头文件

/* =========================================================================
 * 一、内部扩展结构体与全局上下文
 * ========================================================================= */

/* 头文件里的 station_ctx_t 缺少去重和乱序字段，在此做内部扩展 */
typedef struct
{
    station_ctx_t base;                 /* 复用滑动窗口基础结构体（包含6个窗口） */
    uint32_t last_valid_timestamp_sec;  /* 上一次有效数据的时间戳（秒） */
    uint32_t last_valid_timestamp_ms;   /* 上一次有效数据的时间戳（毫秒） */
    env_frame_t reorder_buf[5];         /* 乱序重排缓存区，最多暂存5帧 */
    uint8_t reorder_count;              /* 当前缓存区里暂存了多少帧 */
    int has_valid_timestamp;            /* 标记是否已经收到过有效时间戳（处理初始时间戳为0的情况） */
} station_internal_ctx_t;

#define MAX_STATION_ID 65536
/* 全局上下文数组：用 station_id 做下标，实现 O(1) 查找 */
static station_internal_ctx_t *g_internal_ctxs[MAX_STATION_ID];

/* 全局互斥锁：保护 g_internal_ctxs 的并发分配，防止多线程同时 calloc 导致内存泄漏 */
static pthread_mutex_t g_ctx_lock = PTHREAD_MUTEX_INITIALIZER;

/* 线程池传参结构体：用于把 Q1、Q2 和退出标志传递给工作线程 */
typedef struct {
    ring_queue_t *q1;      /* 输入队列指针（存放原始脏数据） */
    ring_queue_t *q2;      /* 输出队列指针（存放清洗后的干净数据） */
    int *shutdown_flag;    /* 优雅退出标志指针 */
} cleaner_args_t;

/* =========================================================================
 * 二、初始化与销毁
 * ========================================================================= */

/*
 * 函数名称：cleaner_init
 * 功能描述：初始化清洗模块，清空所有站点的内部上下文
 */
void cleaner_init(void)
{
    // 将全局指针数组全部清零，保证启动时所有站点都未分配
    memset(g_internal_ctxs, 0, sizeof(g_internal_ctxs));
}

/*
 * 函数名称：cleaner_destroy
 * 功能描述：释放所有站点的动态分配内存，防止内存泄漏
 * 注意：需在主控模块退出时（所有线程 join 完毕后）调用
 */
void cleaner_destroy(void)
{
    pthread_mutex_lock(&g_ctx_lock); // 加锁保护，防止并发访问

    // 遍历所有可能的站点ID
    for (int i = 0; i < MAX_STATION_ID; i++)
    {
        if (g_internal_ctxs[i] != NULL) // 如果该站点曾经被分配过内存
        {
            free(g_internal_ctxs[i]);    // 释放动态分配的内存
            g_internal_ctxs[i] = NULL;   // 指针置空，防止野指针
        }
    }

    pthread_mutex_unlock(&g_ctx_lock); // 解锁
    pthread_mutex_destroy(&g_ctx_lock); // 销毁互斥锁（此函数只能在所有线程退出后调用）
}

/* =========================================================================
 * 三、上下文获取（线程安全）
 * ========================================================================= */

/*
 * 函数名称：get_internal_ctx
 * 功能描述：获取指定站点的内部上下文，不存在则动态创建
 * 核心：使用双重检查锁定（Double-Checked Locking），兼顾性能与线程安全。
 */
static station_internal_ctx_t *get_internal_ctx(uint16_t station_id)
{
    // 第一次检查（无锁）：如果已经存在，直接返回，避免加锁开销
    if (g_internal_ctxs[station_id] == NULL)
    {
        pthread_mutex_lock(&g_ctx_lock); // 加锁，准备创建

        // 第二次检查（加锁后）：防止多个线程同时判断为空，重复创建
        if (g_internal_ctxs[station_id] == NULL)
        {
            // 使用 calloc 分配内存，并自动清零（保证窗口指针、计数等初始为0）
            g_internal_ctxs[station_id] = (station_internal_ctx_t *)calloc(1, sizeof(station_internal_ctx_t));

            if (g_internal_ctxs[station_id] != NULL)
            {
                // 调用算法层初始化，清空该站点的6个滑动窗口，并设置站点ID
                station_ctx_reset(&g_internal_ctxs[station_id]->base, station_id);
            }
         }

        pthread_mutex_unlock(&g_ctx_lock); // 解锁
    }

    return g_internal_ctxs[station_id]; // 返回站点上下文指针
}

/* =========================================================================
 * 四、核心清洗逻辑（单帧处理）
 * ========================================================================= */

/*
 * 函数名称：process_valid_frame
 * 功能描述：处理“已确认合法（不重复、不乱序）”的帧，
 *           执行物理量程校验、6参数异常检测、窗口更新
 */
static clean_result_t process_valid_frame(station_internal_ctx_t *ctx, env_frame_t *frame) {

    /* 1. 物理量程硬校验：防止 0xFFFF 等错误码污染窗口 */
    if (frame->pm25 > 1000 || frame->pm10 > 1000) { stats_on_clean(2, 1); return CLEAN_DROP; }//PM 越界
    if (frame->temperature < -400 || frame->temperature > 800) { stats_on_clean(2, 1); return CLEAN_DROP; }//温度越界
    if (frame->humidity > 1000) { stats_on_clean(2, 1); return CLEAN_DROP; }//湿度越界
    if (frame->co_ppb > 50000) { stats_on_clean(2, 1); return CLEAN_DROP; }//CO 越界
    if (frame->noise_db > 1500) { stats_on_clean(2, 1); return CLEAN_DROP; }//噪声越界

    /* 2. 一次性对6个参数进行 3σ 检测（批量版本，比6次单独调用更高效） */
    if (anomaly_detect_all(&ctx->base, frame))
	{
        // 如果检测到异常，打印日志并丢弃整帧
        stats_on_clean(1, 1);//突变/异常统计
        return CLEAN_DROP;
    }

    /* 3. 全部参数正常，将新数据写入对应的6个滑动窗口 */
    ctx->base.window_pm25[ctx->base.w_idx] = (int16_t)frame->pm25;
    ctx->base.window_pm10[ctx->base.w_idx] = (int16_t)frame->pm10;
    ctx->base.window_temp[ctx->base.w_idx] = frame->temperature;
    ctx->base.window_hum[ctx->base.w_idx]  = (int16_t)frame->humidity;
    ctx->base.window_co[ctx->base.w_idx]   = (int16_t)frame->co_ppb;
    ctx->base.window_noise[ctx->base.w_idx]= (int16_t)frame->noise_db;

    /* 环形写指针后移：w_idx 加1，如果到达 WINDOW_SIZE 则回到 0 */
    ctx->base.w_idx = (ctx->base.w_idx + 1) % WINDOW_SIZE;

    /* 窗口未满时，有效数据计数加1（直到等于 WINDOW_SIZE 为止） */
    if (ctx->base.w_count < WINDOW_SIZE) {
        ctx->base.w_count++;
    }

    /* 4. 更新去重状态（记录最后一个有效帧的时间戳，用于后续去重） */
    ctx->last_valid_timestamp_sec = frame->timestamp_sec;
    ctx->last_valid_timestamp_ms = frame->timestamp_ms;
    ctx->has_valid_timestamp = 1; // 标记已收到有效时间戳

    return CLEAN_PASS; // 正常放行
}

/*
 * 函数名称：cleaner_process
 * 功能描述：核心清洗逻辑（单帧处理入口），执行去重、乱序重排、异常检测
 */
clean_result_t cleaner_process(env_frame_t *frame) {
    if (frame == NULL) return CLEAN_DROP; // 空指针防御

    /* 1. 告警帧特权：告警帧直接放行，不参与任何清洗和过滤，保证永不丢弃 */
    if (frame->frame_type == FRAME_ALARM) {
        return CLEAN_ALARM;
    }

    /* 2. 心跳帧处理：模拟器会发心跳帧，绝不能进入滑动窗口，否则会污染基线 */
    if (frame->frame_type == FRAME_HEART) {
        stats_on_clean(4, 1);//计入 reorder/其它丢弃桶，便于对账
        return CLEAN_DROP; // 清洗层直接过滤掉心跳帧，不往后传
    }

    // 获取（或创建）该站点的内部上下文
    station_internal_ctx_t *ctx = get_internal_ctx(frame->station_id);
    if (ctx == NULL) return CLEAN_DROP; // 内存分配失败等极端情况

    /* 3. 去重判定：时间戳倒退或等于上次有效时间，说明是重复帧或过期帧 */
    if (ctx->has_valid_timestamp) {
        if (frame->timestamp_sec < ctx->last_valid_timestamp_sec ||
            (frame->timestamp_sec == ctx->last_valid_timestamp_sec &&
             frame->timestamp_ms <= ctx->last_valid_timestamp_ms)) {
            stats_on_clean(3, 1);//重复丢弃统计
            return CLEAN_DROP; // 重复帧，丢弃
        }
    }

    /* 4. 乱序重排判定：时间戳跳跃超过10秒（说明中间有大量丢包或乱序） */
    if (ctx->has_valid_timestamp && frame->timestamp_sec > ctx->last_valid_timestamp_sec + 10) {
        if (ctx->reorder_count < 5) {
            /* 缓存区没满：暂存当前帧，本次返回丢弃（不进入Q2），等待后续帧到来 */
            ctx->reorder_buf[ctx->reorder_count++] = *frame;
            return CLEAN_DROP;
        } else {
            /* 缓存区满了，找出最旧的帧，用当前帧替换它，让最旧的帧继续走后续流程 */
            int min_idx = 0;
            for (int i = 1; i < 5; i++) {
                if (ctx->reorder_buf[i].timestamp_sec < ctx->reorder_buf[min_idx].timestamp_sec ||
                    (ctx->reorder_buf[i].timestamp_sec == ctx->reorder_buf[min_idx].timestamp_sec &&
                     ctx->reorder_buf[i].timestamp_ms < ctx->reorder_buf[min_idx].timestamp_ms)) {
                    min_idx = i; // 找到时间戳最小的帧
                }
            }
            /* 交换：让最旧的帧出来继续处理，把当前乱序帧塞进缓存区 */
            env_frame_t oldest_frame = ctx->reorder_buf[min_idx];
            ctx->reorder_buf[min_idx] = *frame;
            *frame = oldest_frame; // 此时 frame 指向最旧的帧，继续往下走
        }
    }

    /* 5. 走到这里，frame 是确定要处理的有效帧 */
    return process_valid_frame(ctx, frame);
}

/* =========================================================================
 * 五、线程池工作函数
 * ========================================================================= */

/*
 * 函数名称：cleaner_worker_task
 * 功能描述：线程池工作函数（消费者任务），从 Q1 批量取数据，清洗后推入 Q2
 */
void *cleaner_worker_task(void *arg) {
    cleaner_args_t *queues = (cleaner_args_t *)arg; // 标准强转，获取参数

    // 参数判空防御
    if (queues == NULL || queues->q1 == NULL || queues->q2 == NULL) {
        return NULL;
    }

    env_frame_t batch[RINGQ_BATCH_MAX]; // 批量取数据的缓冲区

    while (1) {
        // 批量出队：阻塞直到 Q1 有数据；g_running=0 且空时返回 0
        // 返回实际取出的帧数，存入 batch 数组
        uint32_t n = ring_pop_batch(queues->q1, batch, RINGQ_BATCH_MAX);

        if (n == 0) {
            // 如果取出0帧，说明队列空或收到退出信号
            if (queues->shutdown_flag && *(queues->shutdown_flag)) break; // 收到退出信号，跳出循环
            continue; // 暂时无数据，继续等待
        }

        // 遍历本次取出的每一帧
        for (uint32_t i = 0; i < n; i++) {
            // 调用核心清洗逻辑，返回值表示清洗结果
            clean_result_t result = cleaner_process(&batch[i]);

            // 只要是通过的帧（正常数据或告警帧），就推入 Q2
            if (result == CLEAN_PASS || result == CLEAN_ALARM) {
                // 非阻塞入队：如果 Q2 已满，不会卡死清洗线程，直接记录丢帧
                if (ring_push_try(queues->q2, &batch[i]) != IOT_OK) {
                    stats_on_drop(1); // 记录丢帧统计
                }
            }
        }
    }
    return NULL; // 线程结束
}
