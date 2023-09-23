#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct EngineIterator EngineIterator;

struct EngineIterator
{
	Row*         current;
	Part*        part;
	Locker*      lock;
	HeapIterator iterator;
	Engine*      engine;
};

hot static inline void
engine_iterator_open(EngineIterator* self, Engine* engine)
{
	// lock first partition
	self->current = NULL;
	self->engine  = engine;

	auto ref   = part_map_get_first(&engine->map);
	self->lock = lock_lock(&ref->lock, true);
	self->part = ref->part;

	heap_iterator_open(&self->iterator, &self->part->heap);
	if (unlikely(! heap_iterator_has(&self->iterator)))
	{
		lock_unlock(self->lock);
		self->lock = NULL;
		self->part = NULL;
		return;
	}
	self->current = heap_iterator_at(&self->iterator);
}

static inline void
engine_iterator_close(EngineIterator* self)
{
	self->current = NULL;
	self->part    = NULL;
	if (self->lock)
	{
		lock_unlock(self->lock);
		self->lock = NULL;
	}
}

hot static inline Row*
engine_iterator_at(EngineIterator* self)
{
	return self->current;
}

hot static inline bool
engine_iterator_has(EngineIterator* self)
{
	return self->current != NULL;
}

hot static inline void
engine_iterator_next(EngineIterator* self)
{
	if (unlikely(self->lock == NULL))
		return;

	// iterate current partition
	self->current = NULL;
	heap_iterator_next(&self->iterator);
	if (likely(heap_iterator_has(&self->iterator)))
	{
		self->current = heap_iterator_at(&self->iterator);
		return;
	}

	// get next partition lock
	auto ref = part_map_next(&self->engine->map, self->part);

	// close previous partition lock
	lock_unlock(self->lock);
	self->part = NULL;
	self->lock = NULL;

	// last partition
	if (ref == NULL)
		return;

	// get shared lock
	self->lock = lock_lock(&ref->lock, true);
	self->part = ref->part;

	// open next partition iterator
	heap_iterator_open(&self->iterator, &self->part->heap);
	if (unlikely(! heap_iterator_has(&self->iterator)))
	{
		lock_unlock(self->lock);
		self->lock = NULL;
		self->part = NULL;
		return;
	}
	self->current = heap_iterator_at(&self->iterator);
}

static inline void
engine_iterator_reset(EngineIterator* self)
{
	engine_iterator_close(self);
	self->engine = NULL;
}

static inline void
engine_iterator_init(EngineIterator* self)
{
	self->lock    = NULL;
	self->part    = NULL;
	self->current = NULL;
	self->engine  = NULL;
	heap_iterator_init(&self->iterator);
}
