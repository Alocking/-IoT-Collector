//接入层解码入口 + TCP流式切帧
//  1) decode_wire: 线路字节 → magic预判 → frame_decode(CRC) → 主机序帧
//  2) frame_stream: TCP字节流按固定帧长切分,解决半包/粘包,坏字节滑动重同步
//  3) 本文件不重复实现CRC/字节序,统一走 proto 层接口
#include "iot_collector.h"//引入项目总头文件

//接收主接口(接入层):校验 magic+CRC,输出主机序帧
int decode_wire(const uint8_t *buf, size_t len, env_frame_t *out)
{
    if (buf == NULL || out == NULL){//入参判空
        return -1;//空指针返回失败
    }
    if (frame_magic_ok(buf, len) == 0){//先校验帧头magic(快速失败)
        return IOT_ERR_PROTO;//不是合法帧头
    }
    return frame_decode(buf, len, out);//解码:验CRC + frame_ntoh,成功0
}

//清空累积缓冲(连接重建时调用)
void frame_stream_reset(frame_stream_t *st)
{
    if (st == NULL){//判空
        return;
    }
    st->len = 0;//有效长度清零
    st->resync = 0;//保留累计重同步次数,便于统计;若需完全清零可改为0
}

//流式喂入:追加一段字节,按帧长切分并逐帧回调主机序帧
int frame_stream_feed(frame_stream_t *st, const uint8_t *data, size_t n,
                      frame_sink_fn sink, void *arg)
{
    size_t i = 0;//已消费的输入字节
    int decoded = 0;//本回调成功解出的帧数

    if (st == NULL || data == NULL){//判空
        return 0;
    }

    //先把新数据并入累积缓冲(放不下则丢弃过旧部分并计重同步)
    while (i < n) {
        size_t room = sizeof(st->buf) - st->len;//剩余空间
        size_t take = (n - i < room) ? (n - i) : room;//本拷贝量
        if (take == 0) {//缓冲已满仍进数据:丢最旧一字节重同步
            memmove(st->buf, st->buf + 1, st->len - 1);
            st->len--;
            st->resync++;//重同步计数
            continue;//再试拷贝
        }
        memcpy(st->buf + st->len, data + i, take);
        st->len += take;
        i += take;
    }

    //从缓冲头部反复尝试切出完整帧
    while (st->len >= ENV_FRAME_WIRE_SIZE) {//至少一帧长
        env_frame_t frame;//主机序输出帧
        if (frame_magic_ok(st->buf, ENV_FRAME_WIRE_SIZE) == 0) {//帧头不对
            memmove(st->buf, st->buf + 1, st->len - 1);//滑动1字节找下一magic
            st->len--;
            st->resync++;//重同步
            continue;
        }
        if (decode_wire(st->buf, ENV_FRAME_WIRE_SIZE, &frame) != 0) {//magic对但CRC错
            memmove(st->buf, st->buf + 1, st->len - 1);//同样滑动重同步
            st->len--;
            st->resync++;
            continue;
        }
        //完整合法帧:回调给接入层入队
        if (sink != NULL) {
            sink(&frame, arg);//frame 已是主机序
        }
        decoded++;//成功帧数+1
        memmove(st->buf, st->buf + ENV_FRAME_WIRE_SIZE,//消费整帧
                st->len - ENV_FRAME_WIRE_SIZE);
        st->len -= ENV_FRAME_WIRE_SIZE;//缩短缓冲
    }
    return decoded;//返回本次解析出的帧数
}
