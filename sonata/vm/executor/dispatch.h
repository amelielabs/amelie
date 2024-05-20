#pragma once

//
// sonata.
//
// SQL Database for JSON.
//

typedef struct DispatchStmt DispatchStmt;
typedef struct Dispatch     Dispatch;

struct DispatchStmt
{
	ReqList req_list;
	Trx*    set[];
};

struct Dispatch
{
	Trx**   set;
	int     set_size;
	int     stmt_count;
	int     stmt_last;
	Buf     buf;
	Router* router;
};

static inline void
dispatch_set(Dispatch* self, int order, Trx* trx)
{
	self->set[order] = trx;
}

static inline Trx*
dispatch_get(Dispatch* self, int order)
{
	return self->set[order];
}

static inline DispatchStmt*
dispatch_stmt_at(Dispatch* self, int stmt)
{
	int offset = self->set_size + stmt * (sizeof(DispatchStmt) + self->set_size);
	return (DispatchStmt*)(self->buf.start + offset);
}

static inline DispatchStmt*
dispatch_stmt_set(Dispatch* self, int stmt, int order, Trx* trx)
{
	auto ref = dispatch_stmt_at(self, stmt);
	ref->set[order] = trx;
	return ref;
}

static inline Trx*
dispatch_stmt_get(Dispatch* self, int stmt, int order)
{
	auto ref = dispatch_stmt_at(self, stmt);
	return ref->set[order];
}

static inline void
dispatch_init(Dispatch* self, Router* router)
{
	self->set        = NULL;
	self->set_size   = 0;
	self->stmt_count = 0;
	self->stmt_last  = -1;
	self->router     = router;
	buf_init(&self->buf);
}

static inline void
dispatch_reset(Dispatch* self, ReqCache* cache)
{
	for (int i = 0; i < self->stmt_count; i++)
	{
		auto stmt = dispatch_stmt_at(self, i);
		req_cache_push_list(cache, &stmt->req_list);
	}
	self->set        = NULL;
	self->set_size   = 0;
	self->stmt_count = 0;
	self->stmt_last  = -1;
	buf_reset(&self->buf);
}

static inline void
dispatch_free(Dispatch* self)
{
	buf_free(&self->buf);
}

static inline void
dispatch_create(Dispatch* self, int stmt_count, int stmt_last)
{
	int size_set = sizeof(Trx*) * self->router->set_size;
	int size = size_set + stmt_count * (sizeof(DispatchStmt) + size_set);
	buf_reserve(&self->buf, size);
	memset(self->buf.start, 0, size);

	self->set        = (Trx**)self->buf.start;
	self->set_size   = size_set;
	self->stmt_count = stmt_count;
	self->stmt_last  = stmt_last;
	for (int i = 0; i < stmt_count; i++)
	{
		auto stmt = dispatch_stmt_at(self, i);
		req_list_init(&stmt->req_list);
	}
}

hot static inline void
dispatch_resolve(Dispatch* self, Trx** set)
{
	auto router = self->router;
	for (int i = 0; i < router->set_size; i++)
	{
		if (self->set[i] == NULL)
			continue;
		set[i] = self->set[i];
	}
}
