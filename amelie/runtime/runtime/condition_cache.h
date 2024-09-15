#pragma once

//
// amelie.
//
// Real-Time SQL Database.
//

typedef struct ConditionCache ConditionCache;

struct ConditionCache
{
	List list;
	int  list_count;
};

static inline void
condition_cache_init(ConditionCache* self)
{
	self->list_count = 0;
	list_init(&self->list);
}

static inline void
condition_cache_free(ConditionCache* self)
{
	list_foreach_safe(&self->list)
	{
		auto event = list_at(Condition, link);
		condition_detach(event);
		am_free(event);
	}
}

static inline Condition*
condition_cache_pop(ConditionCache* self)
{
	Condition* event = NULL;
	if (unlikely(self->list_count > 0))
	{
		auto first = list_pop(&self->list);
		self->list_count--;
		event = container_of(first, Condition, link);
	}
	return event;
}

static inline void
condition_cache_push(ConditionCache* self, Condition* event)
{
	list_init(&event->link);
	list_append(&self->list, &event->link);
	self->list_count++;
}

static inline Condition*
condition_cache_create(ConditionCache* self)
{
	auto event = condition_cache_pop(self);
	if (event)
	{
		event_init(&event->event);
		return event;
	}
	event = am_malloc(sizeof(Condition));
	condition_init(event);
	return event;
}
