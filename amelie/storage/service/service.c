
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

void
service_init(Service* self)
{
	self->current    = NULL;
	self->list       = NULL;
	self->list_count = 0;
	self->id         = 0;
	self->waiters    = NULL;
	self->list_count = 0;
	self->subs_count = 0;
	self->errors     = 0;
	spinlock_init(&self->lock);
	list_init(&self->subs);
}

void
service_free(Service* self)
{
	auto page = self->list;
	while (page)
	{
		auto next = page->next;
		service_page_free(page);
		page = next;
	}
	spinlock_free(&self->lock);
}

void
service_attach(Service* self, Subscriber* sub)
{
	spinlock_lock(&self->lock);
	list_append(&self->subs, &sub->link);
	self->subs_count++;
	spinlock_unlock(&self->lock);
}

hot void
service_add(Service*  self, ActionType type,
            Uuid*     db,
            Complete* complete,
            uint32_t  data_size,
            void*     data)
{
	spinlock_lock(&self->lock);
	defer(spinlock_unlock, &self->lock);

	auto size_page = 256 * 1024;
	auto size = sizeof(Action) + data_size;

	// ensure size can fit the page
	if (unlikely(size > (size_page - sizeof(ServicePage))))
		error("service: max page capacity {d} exceeded", size_page);

	// add new page
	auto page = self->current;
	if (unlikely(!page || (page->size - page->position) < size))
	{
		page = service_page_allocate(size_page);
		if (self->current)
			self->current->next = page;
		else
			self->list = page;
		self->list_count++;
		self->current = page;
	}

	// write action
	assert((page->size - page->position) >= size);
	auto action = (Action*)((uint8_t*)page + page->position);
	action->id        = self->id++;
	action->type      = type;
	if (db)
		action->db        = *db;
	else
		uuid_init(&action->db);
	action->complete  = complete;
	action->data_size = data_size;
	if (data_size > 0)
		memcpy(action->data, data, data_size);
	page->position += size;
	page->last      = action;

	// resume waiters
	auto sub = self->waiters;
	self->waiters = NULL;

	// resume waiters
	while (sub)
	{
		auto next = sub->wait_next;
		sub->wait_next = NULL;
		event_signal(&sub->event);
		sub = next;
	}
}

Action*
service_next(Service* self, Subscriber* sub)
{
	// iterate current page slice
	if (likely(sub->pos < sub->end))
	{
		sub->pos = (Action*)(sub->pos->data + sub->pos->data_size);
		return sub->pos;
	}

	auto event = &sub->event;
	event_attach(&sub->event);

	Action* action = NULL;
	for (;;)
	{
		spinlock_lock(&self->lock);

		if (likely(sub->end))
		{
			assert(sub->pos == sub->end);

			// todo: ack?

			// iterate current page
			auto page = sub->page;
			if (sub->end < page->last)
			{
				sub->end = page->last;
				spinlock_unlock(&self->lock);
				break;
			}

			// next page
			if (page->next)
			{
				sub->page = page->next;
				sub->pos  = (Action*)sub->page->data;
				sub->end  = (Action*)sub->page->last;
				spinlock_unlock(&self->lock);
				break;
			}

		} else
		{
			// first iteration
			if (self->list)
			{
				sub->page = self->list;
				sub->pos  = (Action*)sub->page->data;
				sub->end  = (Action*)sub->page->last;
				spinlock_unlock(&self->lock);
				break;
			}
		}

		// (wait for updates)
		sub->wait_next = self->waiters;
		self->waiters  = sub;

		spinlock_unlock(&self->lock);

		// wait
		event_wait(event, -1);
	}

	return action;
}
