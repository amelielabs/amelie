#pragma once

//
// amelie.
//
// Real-Time SQL Database.
//

typedef struct DispatchStmt DispatchStmt;
typedef struct Dispatch     Dispatch;

struct DispatchStmt
{
	ReqList req_list;
	Pipe*   set[];
};

struct Dispatch
{
	int sent;
	int stmt_size;
	int stmt_count;
	int stmt_last;
	Buf stmt;
};

always_inline static inline DispatchStmt*
dispatch_stmt(Dispatch* self, int stmt)
{
	auto offset = stmt * self->stmt_size;
	return (DispatchStmt*)(self->stmt.start + offset);
}

always_inline static inline Pipe*
dispatch_pipe(Dispatch* self, int stmt, int order)
{
	auto ref = dispatch_stmt(self, stmt);
	return ref->set[order];
}

always_inline static inline DispatchStmt*
dispatch_pipe_set(Dispatch* self, int stmt, int order, Pipe* pipe)
{
	auto ref = dispatch_stmt(self, stmt);
	ref->set[order] = pipe;
	return ref;
}

static inline void
dispatch_init(Dispatch* self)
{
	self->sent       = 0;
	self->stmt_count = 0;
	self->stmt_size  = 0;
	self->stmt_last  = -1;
	buf_init(&self->stmt);
}

static inline void
dispatch_reset(Dispatch* self, ReqCache* cache)
{
	for (int i = 0; i < self->stmt_count; i++)
	{
		auto stmt = dispatch_stmt(self, i);
		req_cache_push_list(cache, &stmt->req_list);
	}
	self->sent       = 0;
	self->stmt_count = 0;
	self->stmt_size  = 0;
	self->stmt_last  = -1;
	buf_reset(&self->stmt);
}

static inline void
dispatch_free(Dispatch* self)
{
	buf_free(&self->stmt);
}

static inline void
dispatch_create(Dispatch* self, int set_size, int stmt_count, int stmt_last)
{
	self->stmt_count = stmt_count;
	self->stmt_size  = set_size * sizeof(Pipe*) + sizeof(DispatchStmt);
	self->stmt_last  = stmt_last;
	buf_reset(&self->stmt);

	int size = stmt_count * self->stmt_size;
	buf_reserve(&self->stmt, size);
	memset(self->stmt.start, 0, size);

	for (int i = 0; i < stmt_count; i++)
	{
		auto stmt = dispatch_stmt(self, i);
		req_list_init(&stmt->req_list);
	}
}
