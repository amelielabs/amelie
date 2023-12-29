#
# indigo profiling
# 
CC             = clang
RM             = rm
CFLAGS         = -Wall -pthread -O2 -g -fPIE
CFLAGS_INCLUDE = -I. \
                 -I../indigo/main/api
LFLAGS         = -rdynamic -pthread -Wl,-whole-archive \
                  ../indigo/runtime/libindigo-runtime.a \
                  ../indigo/server/libindigo-server.a \
                  ../indigo/storage/libindigo-storage.a \
                  ../indigo/db/libindigo-db.a \
                  ../indigo/query/libindigo-query.a \
				  ../indigo/main/libindigo-main.a \
                  -Wl,-no-whole-archive -lm -lzstd -lssl -lcrypto
SRC            = prof.c
TARGET         = prof
$(TARGET): clean $(OBJECTS)
	@$(CC) $(SRC) $(CFLAGS_INCLUDE) $(CFLAGS) $(LFLAGS) -o $(TARGET)
	@echo $(CC) $(SRC) $(CFLAGS) -o $(TARGET)
clean:
	@$(RM) -f $(TARGET)
