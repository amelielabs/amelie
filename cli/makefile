#
# monotone cli makefile
# 
CC             = clang
RM             = rm
CFLAGS         = -Wall -pthread -O0 -g
CFLAGS_INCLUDE = -I. -I../monotone/main/api
LFLAGS         = -rdynamic -pthread -Wl,-whole-archive \
                  ../monotone/runtime/libmonotone-runtime.a \
                  ../monotone/server/libmonotone-server.a \
                  ../monotone/db/libmonotone-db.a \
                  ../monotone/query/libmonotone-query.a \
				  ../monotone/main/libmonotone-main.a \
                  -Wl,-no-whole-archive -lm -lreadline -lzstd -lssl -lcrypto
SRC            = monotone.c
TARGET         = monotone
$(TARGET): clean $(OBJECTS)
	@$(CC) $(SRC) $(CFLAGS_INCLUDE) $(CFLAGS) $(LFLAGS) -o $(TARGET)
	@echo $(CC) $(SRC) $(CFLAGS) -o $(TARGET)
clean:
	@$(RM) -f $(TARGET)
