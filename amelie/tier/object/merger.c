
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
#include <amelie_row.h>
#include <amelie_heap.h>
#include <amelie_object.h>

void
merger_init(Merger* self)
{
	self->objects_count = 0;
	list_init(&self->objects);
	list_init(&self->writers);
	list_init(&self->writers_cache);
	hasher_init(&self->hasher);
	object_iterator_init(&self->object_iterator);
	heap_iterator_init(&self->heap_iterator);
	merge_iterator_init(&self->merge_iterator);
}

void
merger_free(Merger* self)
{
	merger_reset(self);
	list_foreach_safe(&self->writers_cache)
	{
		auto writer = list_at(Writer, link);
		writer_free(writer);
	}
	hasher_free(&self->hasher);
	object_iterator_free(&self->object_iterator);
	merge_iterator_free(&self->merge_iterator);
}

void
merger_reset(Merger* self)
{
	self->objects_count = 0;
	list_foreach_safe(&self->objects)
	{
		auto object = list_at(Object, link);
		object_free(object);
	}
	list_init(&self->objects);

	list_foreach_safe(&self->writers)
	{
		auto writer = list_at(Writer, link);
		writer_reset(writer);
		list_init(&writer->link);
		list_append(&self->writers_cache, &writer->link);
	}
	list_init(&self->writers);

	hasher_reset(&self->hasher);
	object_iterator_reset(&self->object_iterator);
	heap_iterator_reset(&self->heap_iterator);
	merge_iterator_reset(&self->merge_iterator);
}

static inline Writer*
merger_create_writer(Merger* self)
{
	Writer* writer;
	if (! list_empty(&self->writers_cache))
	{
		auto first = list_pop(&self->writers_cache);
		writer = container_of(first, Writer, link);
	} else
	{
		writer = writer_allocate();
	}
	list_init(&writer->link);
	list_append(&self->writers, &writer->link);
	return writer;
}

static inline Writer*
merger_first_writer(Merger* self)
{
	return container_of(list_first(&self->writers), Writer, link);
}

static inline Object*
merger_first(Merger* self)
{
	if (! self->objects_count)
		return NULL;
	return container_of(list_first(&self->objects), Object, link);
}

static inline Object*
merger_create(Merger* self, MergerConfig* config)
{
	// create object
	auto meta = &config->origin->meta;
	Id id =
	{
		.id        = (*config->id_seq)++,
		.id_parent = meta->id.id,
		.id_table  = meta->id.id_table
	};
	auto object = object_allocate(config->source, &id);
	list_append(&self->objects, &object->link);
	self->objects_count++;

	// create object file
	object_create(object, ID);
	object_set(object, ID);
	return object;
}

static inline void
merger_drop(Merger* self, Object* object)
{
	list_unlink(&object->link);
	self->objects_count--;
	defer(object_free, object);
	object_delete(object, ID);
}

hot static inline void
merger_write(Writer*       writer,
             Hasher*       hasher,
             MergerConfig* config,
             Iterator*     it,
             Object*       object,
             uint64_t      object_limit)
{
	auto origin = config->origin;
	auto heap   = config->heap;

	writer_reset(writer);
	writer_start(writer, config->source, &object->file, config->keys,
	             hasher);
	for (;;)
	{
		auto row = iterator_at(it);
		if (unlikely(row == NULL))
			break;
		if (row->is_delete)
		{
			iterator_next(it);
			continue;
		}
		writer_add(writer, row);
		if (writer_is_limit(writer, object_limit))
			break;
		iterator_next(it);
	}
	uint64_t lsn = heap->lsn_max;
	if (origin->meta.lsn > lsn)
		lsn = origin->meta.lsn;

	writer_stop(writer, &object->id, 0, lsn);

	// copy and set meta data
	meta_writer_copy(&writer->meta_writer, &object->meta,
	                 &object->meta_data);
}

hot static void
merger_snapshot(Merger* self, MergerConfig* config)
{
	// prepare heap iterator
	auto it = &self->heap_iterator;
	heap_iterator_open(it, config->heap, NULL);

	// create object
	auto object = merger_create(self, config);

	// prepare hasher
	auto hasher = &self->hasher;
	if (! config->origin_hash)
		hasher = NULL;

	// write object
	auto writer = merger_create_writer(self);
	merger_write(writer, hasher, config, &it->it, object, UINT64_MAX);

	// delete the result object if it is empty
	if (! object->meta.rows)
		merger_drop(self, object);
}

hot void
merger_refresh(Merger* self, MergerConfig* config)
{
	// todo: create heap index

	// prepare heap iterator
	heap_iterator_open(&self->heap_iterator, config->heap, NULL);

	// prepare object iterator
	object_iterator_reset(&self->object_iterator);
	object_iterator_open(&self->object_iterator, config->keys, config->origin, NULL);

	// prepare merge iterator
	auto it = &self->merge_iterator;
	merge_iterator_reset(it);
	merge_iterator_add(it, &self->heap_iterator.it);
	merge_iterator_add(it, &self->object_iterator.it);
	merge_iterator_open(it, config->keys);

	// create object
	auto object = merger_create(self, config);

	// prepare hasher
	auto hasher = &self->hasher;
	if (! config->origin_hash)
		hasher = NULL;

	// write object
	auto writer = merger_create_writer(self);
	merger_write(writer, hasher, config, &it->it, object, UINT64_MAX);

	// delete the result object if it is empty
	if (! object->meta.rows)
		merger_drop(self, object);
}

hot static void
merger_split_by_range(Merger* self, MergerConfig* config)
{
	// post refresh partition split by range
	auto first = merger_first(self);

	// prepare object iterator
	auto it = &self->object_iterator;
	object_iterator_reset(it);
	object_iterator_open(it, config->keys, first, NULL);

	// set partition file size limit
	auto limit = config->source->part_size / 2;

	// get writer
	auto writer = merger_first_writer(self);
	while (object_iterator_has(it))
	{
		// create object
		auto object = merger_create(self, config);

		// write object
		writer_reset(writer);
		merger_write(writer, NULL, config, &it->it, object, limit);
	}
}

hot static void
merger_split_by_hash(Merger* self, MergerConfig* config)
{
	// post refresh partition split by matching hash partitions
	auto first = merger_first(self);

	// prepare object iterator
	auto it = &self->object_iterator;
	object_iterator_reset(it);
	object_iterator_open(it, config->keys, first, NULL);

	// set partition file size limit
	auto limit = config->source->part_size / 2;

	// group hash partitions by size
	auto hasher = &self->hasher;
	hasher_group_by(hasher, limit);

	// prepare objects and writers
	auto n = hasher->groups_count;
	assert(n > 1);
	Object* objects[n];
	Writer* writers[n];

	objects[0] = merger_create(self, config);
	writers[0] = merger_first_writer(self);
	writer_reset(writers[0]);
	writer_start(writers[0], config->source, &objects[0]->file, config->keys, NULL);
	for (auto i = 1; i < n; i++)
	{
		objects[i] = merger_create(self, config);
		writers[i] = merger_create_writer(self);
		writer_start(writers[i], config->source, &objects[i]->file, config->keys, NULL);
	}

	// redistribute rows between writers
	while (object_iterator_has(it))
	{
		auto row   = object_iterator_at(it);
		auto group = hasher_match(hasher, row);
		assert(group);
		writer_add(writers[group->pos], row);
	}

	// copy and set meta data
	for (auto i = 0; i < n; i++)
	{
		writer_stop(writers[i], &objects[i]->id, 0, first->meta.lsn);
		meta_writer_copy(&writers[i]->meta_writer, &objects[i]->meta,
		                 &objects[i]->meta_data);
	}
}

hot void
merger_execute(Merger* self, MergerConfig* config)
{
	if (config->type == MERGER_SNAPSHOT)
	{
		merger_snapshot(self, config);
		return;
	}

	// always refresh partition first
	merger_refresh(self, config);
	if (config->type == MERGER_REFRESH)
		return;

	// delete after merge
	auto object = merger_first(self);
	if (! object)
		return;

	// created object fits partition size limit
	if (object->meta.size_total <= (uint64_t)config->source->part_size)
		return;

	// split object file into N objects
	if (! config->origin_hash)
		merger_split_by_range(self, config);
	else
		merger_split_by_hash(self, config);

	// drop first object (created by refresh)
	merger_drop(self, object);
}
