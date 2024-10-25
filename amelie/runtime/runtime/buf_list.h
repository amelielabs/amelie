#pragma once

//
// amelie.
//
// Real-Time SQL OLTP Database.
//
// Copyright (c) 2024 Dmitry Simonenko.
// Copyright (c) 2024 Amelie Labs.
//
// AGPL-3.0 Licensed.
//

typedef struct BufList BufList;

struct BufList
{
	List list;
	int  list_count;
};

static inline void
buf_list_init(BufList* self)
{
	self->list_count = 0;
	list_init(&self->list);
}

hot static inline void
buf_list_free(BufList* self)
{
	list_foreach_safe(&self->list)
	{
		auto buf = list_at(Buf, link);
		buf_cache_push(buf->cache, buf);
	}
	buf_list_init(self);
}

static inline void
buf_list_add(BufList* self, Buf* buf)
{
	list_append(&self->list, &buf->link);
	self->list_count++;
}

static inline void
buf_list_delete(BufList* self, Buf* buf)
{
	list_unlink(&buf->link);
	self->list_count--;
}

static inline Buf*
buf_list_pop(BufList* self)
{
	Buf* buf = NULL;
	if (self->list_count > 0)
	{
		auto next = list_pop(&self->list);
		self->list_count--;
		buf = container_of(next, Buf, link);
	}
	return buf;
}
