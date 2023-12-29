#pragma once

//
// indigo
//
// SQL OLTP database
//

typedef struct Locker      Locker;
typedef struct LockerCache LockerCache;

struct Locker
{
	int        refs;
	void*      lock;
	bool       shared;
	Coroutine* coroutine;
	List       link;
};

struct LockerCache
{
	List list;
	int  list_count;
};

static inline void
locker_cache_init(LockerCache* self)
{
	self->list_count = 0;
	list_init(&self->list);
}

static inline void
locker_cache_free(LockerCache* self)
{
	list_foreach_safe(&self->list)
	{
		auto locker = list_at(Locker, link);
		in_free(locker);
	}
}

static inline Locker*
locker_cache_pop(LockerCache* self)
{
	Locker* locker = NULL;
	if (self->list_count > 0)
	{
		auto first = list_pop(&self->list);
		locker = container_of(first, Locker, link);
		self->list_count--;
	} else
	{
		locker = in_malloc(sizeof(Locker));
		list_init(&locker->link);
	}
	locker->refs      = 0;
	locker->lock      = NULL;
	locker->shared    = false;
	locker->coroutine = in_self();
	return locker;
}

static inline void
locker_cache_push(LockerCache* self, Locker* locker)
{
	locker->refs      = 0;
	locker->lock      = NULL;
	locker->shared    = false;
	locker->coroutine = NULL;
	self->list_count++;
	list_append(&self->list, &locker->link);
}

static inline Locker*
locker_create(LockerCache* self, void* lock, bool shared)
{
	auto locker = locker_cache_pop(self);
	locker->lock   = lock;
	locker->shared = shared;
	return locker;
}
