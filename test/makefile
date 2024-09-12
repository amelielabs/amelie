#
# amelie test makefile
# 
CC             = clang
RM             = rm
CFLAGS         = -Wall -pthread -O0 -g
CFLAGS_INCLUDE = -I../amelie/runtime \
                 -I../amelie/runtime/runtime \
                 -I../amelie/runtime/io \
                 -I../amelie/runtime/lib \
                 -I../amelie/runtime/data \
                 -I../amelie/runtime/config \
                 -I../amelie/server \
                 -I../amelie/server/user \
                 -I../amelie/server/auth \
                 -I../amelie/server/http \
                 -I../amelie/server/client \
                 -I../amelie/server/server \
                 -I../amelie/db \
                 -I../amelie/db/row \
                 -I../amelie/db/transaction \
                 -I../amelie/db/index \
                 -I../amelie/db/partition \
                 -I../amelie/db/checkpoint \
                 -I../amelie/db/wal \
                 -I../amelie/db/db \
                 -I../amelie/vm \
                 -I../amelie/vm/value \
                 -I../amelie/vm/aggr \
                 -I../amelie/vm/executor \
                 -I../amelie/vm/vm \
                 -I../amelie/vm/func \
                 -I../amelie/compiler \
                 -I../amelie/compiler/parser \
                 -I../amelie/compiler/planner \
                 -I../amelie/compiler/compiler \
                 -I../amelie/cluster \
                 -I../amelie/cluster/backup \
                 -I../amelie/cluster/repl \
                 -I../amelie/cluster/cluster \
                 -I../amelie/main \
                 -I../amelie/main/frontend \
                 -I../amelie/main/session \
                 -I../amelie/main/system \
                 -I../amelie/main/main \
                 -I. \
                 -Isuite
LFLAGS         = -rdynamic -pthread -Wl,-whole-archive \
                  ../amelie/runtime/libamelie-runtime.a \
                  ../amelie/server/libamelie-server.a \
                  ../amelie/db/libamelie-db.a \
                  ../amelie/vm/libamelie-vm.a \
                  ../amelie/compiler/libamelie-compiler.a \
                  ../amelie/cluster/libamelie-cluster.a \
                  ../amelie/main/libamelie-main.a \
                  -Wl,-no-whole-archive -lm -lcurl -lssl -lcrypto -lpcre2-8
OBJECTS_RUNTIME = runtime/test_task.o \
                  runtime/test_exception.o \
                  runtime/test_palloc.o \
                  runtime/test_buf.o \
                  runtime/test_msg.o \
                  runtime/test_channel.o \
                  runtime/test_cancel.o \
                  runtime/test_rpc.o
OBJECTS         = suite/suite.o \
                  suite/session.o \
                  test.o
TARGET          = amelie-test
$(TARGET): clean $(OBJECTS_RUNTIME) $(OBJECTS)
	@$(CC) $(OBJECTS_RUNTIME) $(OBJECTS) $(SRC) $(CFLAGS_INCLUDE) $(CFLAGS) $(LFLAGS) -o $(TARGET)
	@echo $(CC) $(SRC) $(CFLAGS) -o $(TARGET)
.c.o:
	@$(CC) $(CFLAGS_INCLUDE) $(CFLAGS) -c $< -o $@
	@echo $(CC) -c $<
clean:
	@$(RM) -f $(TARGET) $(OBJECTS) $(OBJECTS_RUNTIME)
