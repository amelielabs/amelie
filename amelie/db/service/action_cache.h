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

typedef struct ActionCache ActionCache;

struct ActionCache
{
	Spinlock lock;
	List     list;
};

static inline void
action_cache_init(ActionCache* self)
{
	spinlock_init(&self->lock);
	list_init(&self->list);
}

static inline void
action_cache_free(ActionCache* self)
{
	list_foreach_safe(&self->list)
	{
		auto action = list_at(Action, link);
		action_free(action);
	}
	list_init(&self->list);
	spinlock_free(&self->lock);
}

static inline Action*
action_cache_pop(ActionCache* self)
{
	spinlock_lock(&self->lock);
	Action* action = NULL;
	if (! list_empty(&self->list))
	{
		auto first = list_pop(&self->list);
		action = container_of(first, Action, link);
	}
	spinlock_unlock(&self->lock);
	return action;
}

static inline void
action_cache_push(ActionCache* self, Action* action)
{
	spinlock_lock(&self->lock);
	list_append(&self->list, &action->link);
	spinlock_unlock(&self->lock);
}

static inline Action*
action_create(ActionCache* self)
{
	auto action = action_cache_pop(self);
	if (unlikely(! action))
		action = action_allocate();
	action_init(action);
	return action;
}
