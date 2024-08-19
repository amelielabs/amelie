#
# amelie server makefile
#
CC             = clang
AR             = ar
RM             = rm
CFLAGS_OPTS    = -Wall -Wextra -g -O0
CFLAGS_INCLUDE = -I../runtime \
                 -I../runtime/runtime \
                 -I../runtime/io \
                 -I../runtime/lib \
                 -I../runtime/data \
                 -I../runtime/config \
                 -I. \
                 -Iuser \
                 -Iauth \
                 -Ihttp \
                 -Iclient \
                 -Iserver
CFLAGS         = $(CFLAGS_INCLUDE) $(CFLAGS_OPTS)
LIB            = libamelie-server.a
OBJECTS        = \
                 user/user_mgr.o \
                 auth/auth_cache.o \
                 auth/auth_state.o \
                 auth/auth.o \
                 http/http.o \
                 http/uri.o \
                 client/client.o \
                 server/server.o
$(LIB): clean $(OBJECTS)
	@$(AR) cr $(LIB) $(OBJECTS)
	@echo $(AR) $(LIB)
.c.o:
	@$(CC) $(CFLAGS) -c $< -o $@
	@echo $(CC) -c $<
clean:
	@$(RM) -f $(OBJECTS) $(LIB)
