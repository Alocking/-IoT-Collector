//滑动窗口 3σ — 三要点
//  1) 每站点 6 参数滑动窗口：pm25/pm10/temp/hum/co/noise，长度 WINDOW_SIZE
//  2) window_stats_all：批量算均值与标准差 σ（两遍循环：先均值后方差）
//  3) anomaly_detect_all：窗口未满放行；|x-μ| > 3σ 判突变（ANOMALY_SIGMA）
#include "iot_collector.h"//引入项目总头文件

/*
 * 函数名称：station_ctx_reset
 * 功能描述：重置站点上下文，清空6个滑动窗口的所有历史数据
 * 参数：ctx - 站点上下文指针；station_id - 站点编号
 */
void station_ctx_reset(station_ctx_t *ctx, uint16_t station_id)
{
    if (ctx == NULL) return; // 防御性编程：如果传入空指针，直接返回，避免崩溃

    // 使用 memset 将整个结构体所在的内存区域全部置为 0
    // 这样6个滑动窗口、写指针 w_idx 和计数器 w_count 都会被重置
    memset(ctx, 0, sizeof(station_ctx_t));

    // 重新贴上身份标签，记录当前站点编号
    ctx->station_id = station_id;
}

/*
 * 函数名称：window_stats_all
 * 功能描述：一次性计算6个参数的均值和标准差（批量版本）
 * 参数：
 *   ctx    - 站点上下文（包含6个窗口数组）
 *   means  - 输出参数：长度为6的均值数组，顺序为 pm25, pm10, temp, hum, co, noise
 *   sigmas - 输出参数：长度为6的标准差数组
 */
void window_stats_all(const station_ctx_t *ctx, double *means, double *sigmas)
{
    // 如果指针为空，直接返回，防止非法访问
    if (ctx == NULL || means == NULL || sigmas == NULL) return;

    uint8_t count = ctx->w_count; // 当前窗口内实际有效的数据个数

    // 如果窗口里还没有任何数据，所有均值和标准差都置0
    if (count == 0)
	{
        for (int i = 0; i < 6; i++)
		{
            means[i] = 0.0;
            sigmas[i] = 0.0;
        }
        return; // 提前结束
    }

    // 把6个窗口数组的首地址放入一个指针数组，方便用循环统一处理
    const int16_t *windows[6] =
	{
        ctx->window_pm25,   // 索引0
        ctx->window_pm10,   // 索引1
        ctx->window_temp,   // 索引2
        ctx->window_hum,    // 索引3
        ctx->window_co,     // 索引4
        ctx->window_noise   // 索引5
    };

    // 外层循环：遍历6个参数
    for (int p = 0; p < 6; p++)
	{
        const int16_t *w = windows[p]; // 当前参数的窗口数组

        // 第一遍循环：计算均值（平均值 = 总和 / 个数）
        double sum = 0.0;
        for (int i = 0; i < count; i++)
		{
            sum += w[i]; // 累加窗口内每个有效数据
        }
        double mean = sum / count; // 得到均值
        means[p] = mean;           // 存入输出数组

        // 第二遍循环：计算标准差（标准差 = 方差开根号）
        double var_sum = 0.0;
        for (int i = 0; i < count; i++)
		{
            double diff = w[i] - mean;     // 每个数据与均值的差
            var_sum += diff * diff;        // 差的平方累加（方差分子）
        }
        sigmas[p] = sqrt(var_sum / count); // 方差 = var_sum / count，再开根号得标准差
    }
}

/*
 * 函数名称：anomaly_detect_all
 * 功能描述：一次性对6个参数进行 3σ 突变检测（批量版本）
 * 参数：
 *   ctx   - 站点上下文
 *   frame - 当前帧（包含6个最新值）
 * 返回值：
 *   0 - 全部正常
 *   1 - 至少有一个参数异常（整帧丢弃）
 */
int anomaly_detect_all(const station_ctx_t *ctx, const env_frame_t *frame)
{
    // 如果上下文或帧指针为空，视为异常帧，直接返回1
    if (ctx == NULL || frame == NULL) return 1;

    // 窗口未满（数据太少），不具备统计学意义，直接放行，不做检测
    if (ctx->w_count < WINDOW_SIZE) {
        return 0;
    }

    // 一次性算出6个参数的均值和标准差
    double means[6], sigmas[6];
    window_stats_all(ctx, means, sigmas);

    // 把当前帧的6个值放入数组，顺序必须与 stats 输出顺序严格一致
    int16_t new_vals[6] =
	{
        (int16_t)frame->pm25,      // 索引0
        (int16_t)frame->pm10,      // 索引1
        frame->temperature,        // 索引2（温度可能为负，直接用int16_t）
        (int16_t)frame->humidity,  // 索引3
        (int16_t)frame->co_ppb,    // 索引4
        (int16_t)frame->noise_db   // 索引5
    };

    // 循环6个参数，逐一判断是否超过 3σ
    for (int p = 0; p < 6; p++) {
        // 如果标准差极小（说明数据几乎没有波动），跳过检测，防止除零或误判
        if (sigmas[p] < 0.001) continue;

        // 核心 3σ 逻辑：绝对偏差是否超过 3 倍标准差
        if (fabs((double)new_vals[p] - means[p]) > ANOMALY_SIGMA * sigmas[p]) {
            return 1; // 只要有一个参数异常，整帧判定为异常
        }
    }

    return 0; // 全部参数正常
}
