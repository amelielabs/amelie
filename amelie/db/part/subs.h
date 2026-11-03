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

typedef struct Subs Subs;

struct Subs
{
	List list;
	int  list_count;
};

static inline void
subs_init(Subs* self)
{
	self->list_count = 0;
	list_init(&self->list);
}

static inline bool
subs_empty(Subs* self)
{
	return !self->list_count;
}

static inline void
subs_add(Subs* self, Sub* sub)
{
	assert(! sub->wait);
	sub->wait = true;
	sub->event.signal = false;
	list_append(&self->list, &sub->link);
	self->list_count++;
}

static inline void
subs_remove(Subs* self, Sub* sub)
{
	assert(sub->wait);
	sub->wait = false;
	list_unlink(&sub->link);
	self->list_count--;
}

static inline void
subs_resume(Subs* self)
{
	while (self->list_count > 0)
	{
		auto sub = container_of(list_pop(&self->list), Sub, link);
		self->list_count--;
		sub->wait = false;
		sub_signal(sub);
	}
	list_init(&self->list);
}

static inline void
subs_cancel(Subs* self)
{
	while (self->list_count > 0)
	{
		auto sub = container_of(list_pop(&self->list), Sub, link);
		self->list_count--;
		sub->wait   = false;
		sub->cancel = true;
		sub_signal(sub);
	}
	list_init(&self->list);
}
