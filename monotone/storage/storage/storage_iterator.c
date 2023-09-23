
//
// monotone
//	
// SQL OLTP database
//

#include <monotone_runtime.h>
#include <monotone_io.h>
#include <monotone_data.h>
#include <monotone_lib.h>
#include <monotone_config.h>
#include <monotone_schema.h>
#include <monotone_mvcc.h>
#include <monotone_engine.h>
#include <monotone_storage.h>

typedef struct StorageIterator StorageIterator;

struct StorageIterator
{
	Iterator       base;
	EngineIterator iterator;
	Engine*        engine;
};

static inline void
storage_iterator_open(Iterator* base, Row* key)
{
	unused(key);
	auto self = (StorageIterator*)base;
	engine_iterator_open(&self->iterator, self->engine);
}

static inline bool
storage_iterator_has(Iterator* base)
{
	auto self = (StorageIterator*)base;
	return engine_iterator_has(&self->iterator);
}

static inline Row*
storage_iterator_at(Iterator* base)
{
	auto self = (StorageIterator*)base;
	return engine_iterator_at(&self->iterator);
}

static inline void
storage_iterator_next(Iterator* base)
{
	auto self = (StorageIterator*)base;
	engine_iterator_next(&self->iterator);
}

static inline void
storage_iterator_close(Iterator* base)
{
	auto self = (StorageIterator*)base;
	engine_iterator_close(&self->iterator);
}

static inline void
storage_iterator_free(Iterator* base)
{
	auto self = (StorageIterator*)base;
	engine_iterator_close(&self->iterator);
	mn_free(base);
}

Iterator*
storage_iterator_allocate(Storage* storage)
{
	StorageIterator* self = mn_malloc(sizeof(StorageIterator));
	self->base.open  = storage_iterator_open;
	self->base.has   = storage_iterator_has;
	self->base.at    = storage_iterator_at;
	self->base.next  = storage_iterator_next;
	self->base.close = storage_iterator_close;
	self->base.free  = storage_iterator_free;
	self->engine     = &storage->engine;
	engine_iterator_init(&self->iterator);
	return &self->base;
}
