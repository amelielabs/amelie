
//
// amelie.
//
// Real-Time SQL Database.
//

#include <amelie_runtime.h>
#include <amelie_io.h>
#include <amelie_lib.h>
#include <amelie_data.h>
#include <amelie_config.h>
#include <amelie_row.h>
#include <amelie_transaction.h>
#include <amelie_index.h>

hot static bool
index_hash_set(Index* arg, Ref* key, Ref* prev)
{
	auto self = index_hash_of(arg);
	return hash_set(&self->hash, key, prev);
}

hot static void
index_hash_update(Index* arg, Ref* key, Ref* prev, Iterator* it)
{
	unused(arg);
	auto hash_it = index_hash_iterator_of(it);
	hash_iterator_replace(&hash_it->iterator, key, prev);
}

hot static void
index_hash_delete(Index* arg, Ref* key, Ref* prev, Iterator* it)
{
	auto self = index_hash_of(arg);
	auto hash_it = index_hash_iterator_of(it);
	memcpy(key, hash_it->iterator.current, self->hash.keys->key_size);
	hash_iterator_delete(&hash_it->iterator, prev);
}

hot static bool
index_hash_delete_by(Index* arg, Ref* key, Ref* prev)
{
	auto self = index_hash_of(arg);
	return hash_delete(&self->hash, key, prev);
}

hot static bool
index_hash_upsert(Index* arg, Ref* key, Iterator* it)
{
	auto self = index_hash_of(arg);
	uint64_t pos = 0;	
	auto exists = hash_get_or_set(&self->hash, key, &pos);
	auto hash_it = index_hash_iterator_of(it);
	hash_iterator_open_at(&hash_it->iterator, &self->hash, pos);
	return exists;
}

hot static bool
index_hash_ingest(Index* arg, Ref* key)
{
	auto self = index_hash_of(arg);
	uint64_t pos = 0;
	return hash_get_or_set(&self->hash, key, &pos);
}

hot static Iterator*
index_hash_iterator(Index* arg)
{
	auto self = index_hash_of(arg);
	return index_hash_iterator_allocate(self);
}

static void
index_hash_truncate(Index* arg)
{
	auto self = index_hash_of(arg);
	hash_free(&self->hash);
	hash_create(&self->hash, &arg->config->keys);
}

static void
index_hash_free(Index* arg)
{
	auto self = index_hash_of(arg);
	hash_free(&self->hash);
	am_free(self);
}

Index*
index_hash_allocate(IndexConfig* config)
{
	auto self = (IndexHash*)am_malloc(sizeof(IndexHash));
	index_init(&self->index, config);
	hash_init(&self->hash);

	auto iface = &self->index.iface;
	iface->set       = index_hash_set;
	iface->update    = index_hash_update;
	iface->delete    = index_hash_delete;
	iface->delete_by = index_hash_delete_by;
	iface->upsert    = index_hash_upsert;
	iface->ingest    = index_hash_ingest;
	iface->iterator  = index_hash_iterator;
	iface->truncate  = index_hash_truncate;
	iface->free      = index_hash_free;

	hash_create(&self->hash, &config->keys);
	return &self->index;
}
