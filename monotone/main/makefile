#
# monotone main makefile
#
CC             = clang
AR             = ar
RM             = rm
CFLAGS_OPTS    = -Wall -Wextra -g -fPIC -O0
CFLAGS_INCLUDE = -I../runtime \
                 -I../runtime/runtime \
                 -I../runtime/io \
                 -I../runtime/data \
                 -I../runtime/lib \
                 -I../runtime/config \
                 -I../server \
                 -I../server/auth \
                 -I../server/client \
                 -I../server/server \
                 -I../storage \
                 -I../storage/schema \
                 -I../storage/mvcc \
                 -I../storage/engine \
                 -I../storage/storage \
                 -I../db \
                 -I../db/wal \
                 -I../db/db \
                 -I. \
                 -Isession \
                 -Icore \
                 -Imain \
                 -Iapi
CFLAGS         = $(CFLAGS_INCLUDE) $(CFLAGS_OPTS)
LIB            = libmonotone-main.a
OBJECTS        = \
                 session/session.o
$(LIB): clean $(OBJECTS)
	@$(AR) cr $(LIB) $(OBJECTS)
	@echo $(AR) $(LIB)
.c.o:
	@$(CC) $(CFLAGS) -c $< -o $@
	@echo $(CC) -c $<
clean:
	@$(RM) -f $(OBJECTS) $(LIB)
