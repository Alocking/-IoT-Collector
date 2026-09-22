# ===========================================================================
# IoT-Collector Makefile（ES-LINK 扩展版）
# ===========================================================================

CC      := gcc
CFLAGS  := -Wall -Wextra -std=c11 -O2 -D_GNU_SOURCE -D_DEFAULT_SOURCE -I.
LDFLAGS := -pthread -lm

TARGET  := iot-collector
TOOLS   := station_sim frame_dump cloud_bridge

SRCS    := main.c                      \
           common/signal_handler.c     \
           common/proto.c              \
           common/stats.c              \
           common/tbf.c                \
           common/thread_pool.c        \
           ingress/decoder.c           \
           ingress/udp_receiver.c      \
           ingress/tcp_poller.c        \
           pipeline/ring_queue.c       \
           pipeline/cleaner.c          \
           pipeline/station_ctx.c      \
           egress/dispatcher.c         \
           egress/unicast_sender.c     \
           egress/mcast_sender.c       \
           egress/cache_writer.c

OBJS    := $(SRCS:.c=.o)
DEPS    := $(SRCS:.c=.d)

.PHONY: all tools clean rebuild
all: $(TARGET) tools

$(TARGET): $(OBJS)
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS)
	@echo "[BUILD] $(TARGET) ok"

tools: station_sim frame_dump cloud_bridge
station_sim: tools/station_sim.c
	$(CC) $(CFLAGS) -o $@ $< -lm
frame_dump: tools/frame_decoder.c
	$(CC) $(CFLAGS) -o $@ $< -lm
cloud_bridge: tools/cloud_bridge.c common/proto.c iot_collector.h
	$(CC) $(CFLAGS) -o $@ tools/cloud_bridge.c common/proto.c $(LDFLAGS)
	@echo "[BUILD] cloud_bridge ok"

%.o: %.c
	@mkdir -p $(dir $@)
	$(CC) $(CFLAGS) -MMD -MP -c -o $@ $<
-include $(DEPS)

clean:
	rm -f $(OBJS) $(DEPS) $(TARGET) $(TOOLS)
	rm -f common/*.[od] ingress/*.[od] pipeline/*.[od] egress/*.[od]
	rm -f *.o *.d *.bin *.dat *.cursor
rebuild: clean all
