#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct Dispatch  Dispatch;

struct Dispatch
{
	Req**         set;
	int           set_size;

	Buf           stmt;
	int           stmt_count;
	int           stmt_current;

	int           complete;
	int           sent;

	Buf*          error;
	CodeData*     code_data;
	Router*       router;
	ReqCache*     cache;
	DispatchLock* lock;
};

void dispatch_init(Dispatch*, DispatchLock*, ReqCache*, Router*, CodeData*);
void dispatch_free(Dispatch*);
void dispatch_reset(Dispatch*);
Req* dispatch_map(Dispatch*, uint32_t, int);
void dispatch_copy(Dispatch*, Code*, int);
void dispatch_export(Dispatch*, LogSet*);

static inline Req*
dispatch_at(Dispatch* self, int order)
{
	return self->set[order];
}

static inline Req*
dispatch_at_stmt(Dispatch* self, int stmt, int order)
{
	assert(stmt < self->stmt_count);
	auto index = (Req**)(self->stmt.start);
	return index[stmt * self->set_size + order];
}
