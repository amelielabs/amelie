#
# sonata cli makefile
# 
CC             = clang
RM             = rm
CFLAGS         = -Wall -pthread -O2 -g
CFLAGS_INCLUDE = -I../sonata/runtime \
                 -I../sonata/runtime/runtime \
                 -I../sonata/runtime/io \
                 -I../sonata/runtime/lib \
                 -I../sonata/runtime/data \
                 -I../sonata/runtime/config \
                 -I../sonata/server \
                 -I../sonata/server/auth \
                 -I../sonata/server/http \
                 -I../sonata/server/client \
                 -I../sonata/server/server \
                 -I../sonata/db \
                 -I../sonata/db/def \
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
                 -I../sonata/compiler/semantic \
                 -I../sonata/compiler/compiler \
                 -I../sonata/main \
                 -I../sonata/main/frontend \
                 -I../sonata/main/shard \
                 -I../sonata/main/session \
                 -I../sonata/main/main \
                 -I.
LFLAGS         = -rdynamic -pthread -Wl,-whole-archive \
                  ../sonata/runtime/libsonata-runtime.a \
                  ../sonata/server/libsonata-server.a \
                  ../sonata/db/libsonata-db.a \
                  ../sonata/vm/libsonata-vm.a \
                  ../sonata/compiler/libsonata-compiler.a \
				  ../sonata/main/libsonata-main.a \
                  -Wl,-no-whole-archive -lm -lcurl -lssl -lcrypto
SRC            = sonata.c
TARGET         = sonata
$(TARGET): clean $(OBJECTS)
	@$(CC) $(SRC) $(CFLAGS_INCLUDE) $(CFLAGS) $(LFLAGS) -o $(TARGET)
	@echo $(CC) $(SRC) $(CFLAGS) -o $(TARGET)
clean:
	@$(RM) -f $(TARGET)
