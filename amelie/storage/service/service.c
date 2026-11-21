
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

#include <amelie_runtime>
#include <amelie_heap>
#include <amelie_transaction.h>
#include <amelie_part.h>
#include <amelie_catalog.h>
#include <amelie_service.h>

always_inline static inline ServicePage*
service_at(Service* self, int order)
{
	return ((ServicePage**)self->list.start)[order];
}

always_inline static inline ServicePage*
service_get(Service* self, uint32_t id)
{
	return service_at(self, id - self->id_first);
}

void
service_init(Service* self)
{
	self->current    = NULL;
	self->last       = NULL;
	self->id         = 0;
	self->id_first   = 0;
	self->waiters    = NULL;
	self->list_count = 0;
	buf_init(&self->list);
	spinlock_init(&self->subs_lock);
	list_init(&self->subs);
}

void
service_free(Service* self)
{
	for (auto i = 0; i < self->list_count; i++)
	{
		auto at = service_at(self, i);
		service_page_free(at);
	}
	buf_free(&self->list);
	spinlock_free(&self->subs_lock);
}

void
service_attach(Service* self, Subscriber* sub)
{
	spinlock_lock(&self->subs_lock);
	list_append(&self->subs, &sub->link);
	spinlock_unlock(&self->subs_lock);
}

void
service_add(Service* self, Action* action)
{
	// todo: skip if action.lsn <= ack?

	auto size_page = 256 * 1024;
	auto size = action_size(action);

	// ensure size can fit the page
	if (unlikely(size > (size_page - sizeof(ServicePage))))
		error("service: max page capacity {u32} exceeded", size_page);

	// add new page
	auto page = self->current;
	if (unlikely(!page || (page->size - page->position) < size))
	{
		page = service_page_allocate(size_page);
		page->id = self->id++;
		buf_write(&self->list, &page, sizeof(ServicePage*));
		self->list_count++;
		self->current = page;
	}

	// write action
	self->last = service_page_write(page, action);
}

void
service_resume(Service* self)
{
	Subscriber* sub;
	spinlock_lock(&self->subs_lock);
	sub = self->waiters;
	self->waiters = NULL;
	// todo: get min
	spinlock_unlock(&self->subs_lock);

	// todo: gc

	// resume waiters
	while (sub)
	{
		auto next = sub->wait_next;
		sub->end       = self->last;
		sub->wait_next = NULL;
		event_signal(&sub->event);

		sub = next;
	}
}

hot static inline Action*
service_next_action(Service* self, Subscriber* sub)
{
	if (sub->start == NULL)
	{
		sub->start = (Action*)service_at(self, 0)->data;
		sub->pos   = sub->start;
		return sub->start;
	}

	auto page = service_get(self, sub->pos->page);
	auto next = (Action*)(sub->pos->data + sub->pos->data_size);
	auto next_offset = (uintptr_t)(next) - (uintptr_t)page;

	// next page
	if (next_offset == page->position)
	{
		page = service_get(self, sub->pos->page + 1);
		next = (Action*)(page->data);
	}

	sub->pos = next;
	return next;
}

Action*
service_next(Service* self, Subscriber* sub)
{
	auto event = &sub->event;
	event_attach(&sub->event);

	Action* action = NULL;
	for (;;)
	{
		if (sub->pos != sub->end)
		{
			action = service_next_action(self, sub);
			if (action)
				break;
		}

		// ack and wait
		spinlock_lock(&self->subs_lock);

		// todo: acknowledge pos for gc

		// check if there are pending events
		if (self->last != sub->end)
		{
			sub->end = self->last;
			spinlock_unlock(&self->subs_lock);
			continue;
		}

		sub->wait_next = self->waiters;
		self->waiters  = sub;

		spinlock_unlock(&self->subs_lock);

		// wait
		event_wait(event, -1);
	}

	return action;
}
