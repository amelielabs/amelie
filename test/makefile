#
# indigo test-suite
# 
CC              = clang
RM              = rm
CFLAGS          = -Wall -pthread -O0 -g -fPIE
CFLAGS_INCLUDE  = -I../indigo/runtime \
                  -I../indigo/runtime/runtime \
                  -I../indigo/runtime/io \
                  -I../indigo/runtime/data \
                  -I../indigo/main/api \
                  -I. \
                  -Isuite
LFLAGS          = -rdynamic -pthread -Wl,-whole-archive \
                   ../indigo/runtime/libindigo-runtime.a \
                   ../indigo/server/libindigo-server.a \
                   ../indigo/storage/libindigo-storage.a \
                   ../indigo/db/libindigo-db.a \
                   ../indigo/query/libindigo-query.a \
                   ../indigo/main/libindigo-main.a \
                   -Wl,-no-whole-archive -lm -lzstd -lssl -lcrypto
TARGET          = test
OBJECTS_RUNTIME = runtime/test_task.o \
                  runtime/test_exception.o \
                  runtime/test_palloc.o \
                  runtime/test_buf.o \
                  runtime/test_msg.o \
                  runtime/test_coroutine.o \
                  runtime/test_event.o \
                  runtime/test_condition.o \
                  runtime/test_channel.o \
                  runtime/test_rpc.o \
                  runtime/test_cancel.o \
                  runtime/test_lock.o \
                  runtime/test_sleep.o \
                  runtime/test_blob.o \
                  runtime/test_tcp.o
OBJECTS         = suite/suite.o \
                  test.o
$(TARGET): clean $(OBJECTS_RUNTIME) $(OBJECTS)
	@$(CC) $(OBJECTS_RUNTIME) $(OBJECTS) $(CFLAGS_INCLUDE) $(CFLAGS) $(LFLAGS) -o $(TARGET)
	@echo $(CC) $(CFLAGS) -o $(TARGET)
.c.o:
	@$(CC) $(CFLAGS_INCLUDE) $(CFLAGS) -c $< -o $@
	@echo $(CC) -c $<
clean:
	@$(RM) -f $(OBJECTS_RUNTIME) $(OBJECTS) $(TARGET)
