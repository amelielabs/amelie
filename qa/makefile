#
# monotone test-suite
# 
CC             = clang
RM             = rm
CFLAGS         = -Wall -pthread -O0 -g -fPIE
CFLAGS_INCLUDE = -I../monotone/runtime \
                 -I../monotone/runtime/runtime \
                 -I../monotone/main/api \
                 -I. \
                 -Isuite
LFLAGS         = -rdynamic -pthread -Wl,-whole-archive \
                  ../monotone/runtime/libmonotone-runtime.a \
                  ../monotone/server/libmonotone-server.a \
                  ../monotone/db/libmonotone-db.a \
                  ../monotone/query/libmonotone-query.a \
                  ../monotone/cluster/libmonotone-cluster.a \
				  ../monotone/main/libmonotone-main.a \
                  -Wl,-no-whole-archive -lm -lzstd -lssl -lcrypto
TARGET         = test
OBJECTS        = suite/suite.o \
                 test.o
$(TARGET): clean $(OBJECTS)
	@$(CC) $(OBJECTS) $(CFLAGS_INCLUDE) $(CFLAGS) $(LFLAGS) -o $(TARGET)
	@echo $(CC) $(CFLAGS) -o $(TARGET)
.c.o:
	@$(CC) $(CFLAGS_INCLUDE) $(CFLAGS) -c $< -o $@
	@echo $(CC) -c $<
clean:
	@$(RM) -f $(OBJECTS) $(TARGET)
