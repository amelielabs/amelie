#
# monotone runtime test suite makefile
#
CC             = clang
RM             = rm
CFLAGS         = -Wall -pthread -O0 -g
CFLAGS_INCLUDE = \
                 -I../monotone/runtime \
                 -I../monotone/runtime/runtime \
                 -I../monotone/runtime/io \
                 -I.
LFLAGS         = -rdynamic -pthread -Wl,-whole-archive \
                  ../monotone/runtime/libmonotone-runtime.a \
                  -Wl,-no-whole-archive -lm -ldl -rdynamic -lssl -lcrypto -lzstd
TARGET         = test
OBJECTS        = test.o \
                 test_task.o \
                 test_exception.o \
                 test_palloc.o \
                 test_buf.o \
                 test_msg.o \
                 test_coroutine.o \
                 test_event.o \
                 test_condition.o \
                 test_channel.o \
                 test_rpc.o \
                 test_cancel.o \
                 test_lock.o \
                 test_sleep.o \
                 test_blob.o \
                 test_tcp.o
$(TARGET): clean $(OBJECTS)
	@$(CC) $(OBJECTS) $(CFLAGS_INCLUDE) $(CFLAGS) $(LFLAGS) -o $(TARGET)
	@echo $(CC) $(CFLAGS) -o $(TARGET)
.c.o:
	@$(CC) $(CFLAGS_INCLUDE) $(CFLAGS) -c $< -o $@
	@echo $(CC) -c $<
clean:
	@$(RM) -f $(OBJECTS) $(TARGET)
