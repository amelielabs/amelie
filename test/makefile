#
# sonata test makefile
# 
CC             = clang
RM             = rm
CFLAGS         = -Wall -pthread -O0 -g
CFLAGS_INCLUDE = -I../sonata/runtime \
                 -I../sonata/runtime/runtime \
                 -I../sonata/runtime/io \
                 -I../sonata/runtime/lib \
                 -I../sonata/runtime/data \
                 -I../sonata/runtime/config \
                 -I../sonata/server \
                 -I../sonata/server/user \
                 -I../sonata/server/http \
                 -I../sonata/server/client \
                 -I../sonata/server/server \
                 -I../sonata/db \
                 -I../sonata/db/row \
                 -I../sonata/db/transaction \
                 -I../sonata/db/index \
                 -I../sonata/db/partition \
                 -I../sonata/db/wal \
                 -I../sonata/db/db \
                 -I../sonata/vm \
                 -I../sonata/vm/value \
                 -I../sonata/vm/aggr \
                 -I../sonata/vm/executor \
                 -I../sonata/vm/vm \
                 -I../sonata/compiler \
                 -I../sonata/compiler/parser \
                 -I../sonata/compiler/planner \
                 -I../sonata/compiler/compiler \
                 -I../sonata/cluster \
                 -I../sonata/cluster/backup \
                 -I../sonata/cluster/repl \
                 -I../sonata/cluster/cluster \
                 -I../sonata/main \
                 -I../sonata/main/frontend \
                 -I../sonata/main/session \
                 -I../sonata/main/main \
                 -I. \
                 -Isuite
LFLAGS         = -rdynamic -pthread -Wl,-whole-archive \
                  ../sonata/runtime/libsonata-runtime.a \
                  ../sonata/server/libsonata-server.a \
                  ../sonata/db/libsonata-db.a \
                  ../sonata/vm/libsonata-vm.a \
                  ../sonata/compiler/libsonata-compiler.a \
                  ../sonata/cluster/libsonata-cluster.a \
                  ../sonata/main/libsonata-main.a \
                  -Wl,-no-whole-archive -lm -lcurl -lssl -lcrypto
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
                  runtime/test_sleep.o
OBJECTS         = suite/suite.o \
                  suite/session.o \
                  test.o
TARGET          = sonata-test
$(TARGET): clean $(OBJECTS_RUNTIME) $(OBJECTS)
	@$(CC) $(OBJECTS_RUNTIME) $(OBJECTS) $(SRC) $(CFLAGS_INCLUDE) $(CFLAGS) $(LFLAGS) -o $(TARGET)
	@echo $(CC) $(SRC) $(CFLAGS) -o $(TARGET)
.c.o:
	@$(CC) $(CFLAGS_INCLUDE) $(CFLAGS) -c $< -o $@
	@echo $(CC) -c $<
clean:
	@$(RM) -f $(TARGET) $(OBJECTS) $(OBJECTS_RUNTIME)
