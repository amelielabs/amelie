#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct Handle Handle;

typedef void (*HandleDestructor)(Handle*, void*);

struct Handle
{
	Str*         name;
	uint64_t     lsn;
	bool         drop;
	Transaction* trx;
	Handle*      next;
	Handle*      prev;
	List         link;
};

static inline void
handle_init(Handle* self)
{
	self->name = NULL;
	self->lsn  = 0;
	self->drop = false;
	self->trx  = NULL;
	self->next = NULL;
	self->prev = NULL;
	list_init(&self->link);
}

static inline void
handle_link(Handle* prev, Handle* handle)
{
	prev->next   = handle;
	handle->prev = prev;
	handle->next = NULL;
}

static inline void
handle_unlink(Handle* self)
{
	if (self->prev)
		self->prev->next = self->next;
	if (self->next)
		self->next->prev = self->prev;
	self->prev = NULL;
	self->next = NULL;
}

static inline void
handle_set_transaction(Handle* self, Transaction* trx)
{
	self->trx = trx;
}

static inline void
handle_commit(Handle* self, uint64_t lsn)
{
	self->lsn = lsn;
	self->trx = NULL;
}

static inline bool
handle_is_commited(Handle* self)
{
	return self->trx == NULL;
}
