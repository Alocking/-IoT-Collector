//信号与优雅退出 — 五要点
//  1) 运行标志 g_running 必须是 volatile sig_atomic_t：信号处理器只改它
//  2) SIGINT/SIGTERM 置 g_running=0，其余工作全部交给主循环/线程协作退出
//  3) SIGPIPE 必须忽略：对端断开时 send 才会返回 EPIPE，而不是直接杀进程
//  4) 处理器内禁止 printf/malloc 等非异步安全函数
//  5) 优雅退出：g_running=0 → ring_wake_all → join 各线程 → 冲刷/关闭资源
#include "iot_collector.h"//引入项目总头文件

volatile sig_atomic_t g_running = 1;//全局运行标志(头文件 extern 声明的唯一定义)

//信号处理函数：只改标志，不做复杂逻辑
static void on_exit_signal(int signo)
{
    (void)signo;//未使用信号编号
    g_running = 0;//通知所有线程进入退出流程
}

//注册 SIGINT/SIGTERM(置 g_running=0)，忽略 SIGPIPE；成功 0，失败 -1
int iot_signal_init(void)
{
    struct sigaction sa;//信号动作结构体

    memset(&sa, 0, sizeof(sa));//清零
    sa.sa_handler = on_exit_signal;//绑定退出处理
    sigemptyset(&sa.sa_mask);//处理期间不额外屏蔽信号
    sa.sa_flags = SA_RESTART;//被信号打断的系统调用自动重启，减少假错

    if (sigaction(SIGINT, &sa, NULL) < 0){//Ctrl+C
        perror("sigaction(SIGINT)");//打印错误
        return -1;//失败
    }
    if (sigaction(SIGTERM, &sa, NULL) < 0){//kill 默认信号
        perror("sigaction(SIGTERM)");//打印错误
        return -1;//失败
    }

    memset(&sa, 0, sizeof(sa));//重新清零
    sa.sa_handler = SIG_IGN;//忽略：管道对端关闭不应杀死采集进程
    sigemptyset(&sa.sa_mask);//无屏蔽
    if (sigaction(SIGPIPE, &sa, NULL) < 0){//忽略 SIGPIPE
        perror("sigaction(SIGPIPE)");//打印错误
        return -1;//失败
    }

    g_running = 1;//启动前确保标志为运行态
    return 0;//注册成功
}

//优雅退出序列占位：具体资源回收由 main.c 按模块逆序完成
void iot_shutdown_sequence(void)
{
    g_running = 0;//先停标志
    //后续：ring_wake_all(Q1/Q2) → pthread_join → 关 socket/缓存 → 最终统计
}
