// 协议编解码：主机内存帧 <-> 线路字节流
//  1) 发送前 frame_hton，接收 CRC 通过后 frame_ntoh
//  2) 线路多字节字段网络序；crc16 按网络序存放
//  3) CRC 在网络序布局上计算，覆盖 magic 至 noise_db
//  4) frame_encode 入参业务字段应已是网络序
//  5) frame_decode 出参业务字段已还原为主机序
#include "iot_collector.h"//引入项目总头文件

uint16_t crc16_ccitt(const uint8_t *data, size_t len){//crc循环冗余校验
    uint16_t crc = 0xffffu;//CRC初值(覆盖magic到noise_db)
    size_t i = 0;//字节循环变量
    int bit = 0;//位循环变量
    if (data == NULL){//判空
        return 0;
    }
    for(i = 0;i < len;i++){//逐字节处理
        crc ^= (uint16_t)data[i] << 8;//字节放到CRC寄存器高8位再参与多项式运算
        for(bit = 0;bit < 8;bit++){//位处理
            if(crc & 0x8000u){//最高位为1
                crc = (uint16_t)((crc << 1) ^ 0x1021u);//左移并异或多项式
            }else{//最高位为0
                crc = (uint16_t)(crc << 1);//仅左移
            }
        }
    }
    return crc;//返回校验值
}

//主机序 → 网络序
void frame_hton(env_frame_t *f)
{
    if (f == NULL){//判空
        return;
    }
    f->station_id = htons(f->station_id);//站点编号
    f->timestamp_sec = htonl(f->timestamp_sec);//时间戳秒
    f->timestamp_ms = htonl(f->timestamp_ms);//时间戳毫秒(0~999)
    f->temperature = (int16_t)htons((uint16_t)f->temperature);//温度(带符号按位转)
    f->humidity = htons(f->humidity);//相对湿度
    f->pm25 = htons(f->pm25);//PM2.5
    f->pm10 = htons(f->pm10);//PM10
    f->co_ppb = htons(f->co_ppb);//CO
    f->noise_db = htons(f->noise_db);//噪声
    //magic/version/frame_type/crc16 为单字节或派生字段,此处不转
}

//网络序 → 主机序(须在 CRC 校验通过之后调用)
void frame_ntoh(env_frame_t *f)
{
    if (f == NULL){//判空
        return;
    }
    f->station_id = ntohs(f->station_id);
    f->timestamp_sec = ntohl(f->timestamp_sec);
    f->timestamp_ms = ntohl(f->timestamp_ms);
    f->temperature = (int16_t)ntohs((uint16_t)f->temperature);
    f->humidity = ntohs(f->humidity);
    f->pm25 = ntohs(f->pm25);
    f->pm10 = ntohs(f->pm10);
    f->co_ppb = ntohs(f->co_ppb);
    f->noise_db = ntohs(f->noise_db);
}

int frame_magic_ok(const uint8_t *buf, size_t len){//判断一段原始字节流是否像一个合法帧
    if(buf == NULL || len < ENV_FRAME_WIRE_SIZE){//长度不足
        return 0;//不合法
    }
    return (buf[0] == FRAME_MAGIC_0 && buf[1] == FRAME_MAGIC_1) ? 1 : 0;//帧头0xEB90
}

//对「网络序帧」验证CRC(刚从线路拷出的原始布局;frame_ntoh 之前调用)
int frame_crc_ok(const env_frame_t *frame)
{
    env_frame_t tmp;//局部副本,crc16字段按0参与
    uint16_t expect = 0;//期望CRC
    if (frame == NULL){//判空
        return 0;
    }
    if (frame->magic[0] != FRAME_MAGIC_0 || frame->magic[1] != FRAME_MAGIC_1){//先查magic
        return 0;
    }
    memcpy(&tmp, frame, sizeof(tmp));//拷贝网络序布局
    tmp.crc16 = 0;//CRC字段不覆盖
    expect = crc16_ccitt((const uint8_t *)&tmp, ENV_FRAME_CRC_SIZE);//对网络序字节算CRC
    return (expect == ntohs(frame->crc16)) ? 1 : 0;//crc16 在线路上为网络序
}

//给「网络序帧」补写 crc16(须在 frame_hton 之后;与 station_sim 一致: htons(CRC))
void frame_fill_crc(env_frame_t *frame)
{
    env_frame_t tmp;
    if (frame == NULL){//判空
        return;
    }
    memcpy(&tmp, frame, sizeof(tmp));
    tmp.crc16 = 0;//先清零再计算
    frame->crc16 = htons(crc16_ccitt((const uint8_t *)&tmp, ENV_FRAME_CRC_SIZE));//网络序写入
}

//发送主接口:打包并补充CRC。入参业务字段应已是网络序(egress 先 frame_hton)
int frame_encode(const env_frame_t *in, uint8_t *buf, size_t buf_len)
{
    env_frame_t wire;//线路帧
    if (in == NULL || buf == NULL || buf_len < ENV_FRAME_WIRE_SIZE){//参数与容量校验
        return -1;//非法返回失败
    }
    wire = *in;//拷贝(已是网络序的业务字段)
    wire.magic[0] = FRAME_MAGIC_0;//补帧头
    wire.magic[1] = FRAME_MAGIC_1;
    if (wire.version == 0){//版本默认1
        wire.version = 1;
    }
    frame_fill_crc(&wire);//按网络序布局补CRC
    memcpy(buf, &wire, ENV_FRAME_WIRE_SIZE);//写出线路字节
    return (int)ENV_FRAME_WIRE_SIZE;//返回写入字节数
}

//接收主接口:校验CRC并解包。出参业务字段已 frame_ntoh 为主机序
int frame_decode(const uint8_t *buf, size_t len, env_frame_t *out)
{
    env_frame_t wire;//网络序原始帧
    if (buf == NULL || out == NULL || len < ENV_FRAME_WIRE_SIZE){//参数与长度校验
        return -1;
    }
    if (frame_magic_ok(buf, len) == 0){//帧头校验
        return IOT_ERR_PROTO;//协议错误
    }
    memcpy(&wire, buf, ENV_FRAME_WIRE_SIZE);//整块拷贝为网络序布局
    if (frame_crc_ok(&wire) == 0){//在网络序布局上验CRC
        return IOT_ERR_PROTO;//CRC错误
    }
    *out = wire;//先给出原始内容
    frame_ntoh(out);//再转主机序,供清洗/分发使用
    return 0;//成功
}
