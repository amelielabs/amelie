
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
	self->writers       = NULL;
	self->writers_cache = NULL;
	self->objects_count = 0;
	list_init(&self->objects);
	buf_init(&self->heap_index);
	object_iterator_init(&self->object_iterator);
	merge_iterator_init(&self->merge_iterator);
}

void
merger_free(Merger* self)
{
	merger_reset(self);
	auto writer = self->writers_cache;
	while (writer)
	{
		auto next = writer->next;
		writer_free(writer);
		writer = next;
	}
	self->writers_cache = NULL;

	buf_free(&self->heap_index);
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

	auto writer = self->writers;
	while (writer)
	{
		auto next = writer->next;
		writer->next = self->writers_cache;
		self->writers_cache = writer;
		writer = next;
	}
	self->writers = NULL;

	buf_reset(&self->heap_index);
	object_iterator_reset(&self->object_iterator);
	merge_iterator_reset(&self->merge_iterator);
}

static inline Writer*
merger_create_writer(Merger* self)
{
	Writer* writer;
	if (self->writers_cache)
	{
		writer = self->writers_cache;
		self->writers_cache = writer->next;
		writer->next = NULL;
	} else
	{
		writer = writer_allocate();
	}
	writer->next = self->writers;
	self->writers = writer;
	return writer;
}

static inline Object*
merger_create(Merger* self, MergerConfig* config)
{
	// create object
	auto meta = &config->origin->meta;
	Id id =
	{
		.id        = state_psn_next(),
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
             MergerConfig* config,
             Iterator*     it,
             Object*       object,
             uint64_t      object_limit,
             uint32_t      hash_min,
             uint32_t      hash_max)
{
	writer_reset(writer);
	writer_start(writer, config->source, &object->file);
	while (iterator_has(it))
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
	writer_stop(writer, &object->id, hash_min, hash_max, 0,
	            config->lsn);

	// copy and set meta data
	meta_writer_copy(&writer->meta_writer, &object->meta,
	                 &object->meta_data);
}

hot static void
merger_snapshot(Merger* self, MergerConfig* config)
{
	auto meta = &config->origin->meta;

	// create heap index
	heap_index(config->heap, config->keys, &self->heap_index);

	// prepare heap index iterator
	HeapIndexIterator it;
	heap_index_iterator_init(&it);
	heap_index_iterator_open(&it, &self->heap_index, NULL);

	// create object
	auto object = merger_create(self, config);

	// write object
	auto writer = merger_create_writer(self);
	merger_write(writer, config, &it.it, object, UINT64_MAX,
	             meta->hash_min,
	             meta->hash_max);
}

hot void
merger_refresh(Merger* self, MergerConfig* config)
{
	auto meta = &config->origin->meta;

	// create heap index
	heap_index(config->heap, config->keys, &self->heap_index);

	// prepare heap index iterator
	HeapIndexIterator heap_it;
	heap_index_iterator_init(&heap_it);
	heap_index_iterator_open(&heap_it, &self->heap_index, NULL);

	// prepare object iterator
	object_iterator_reset(&self->object_iterator);
	object_iterator_open(&self->object_iterator, config->keys, config->origin, NULL);

	// prepare merge iterator
	auto it = &self->merge_iterator;
	merge_iterator_reset(it);
	merge_iterator_add(it, &heap_it.it);
	merge_iterator_add(it, &self->object_iterator.it);
	merge_iterator_open(it, config->keys);

	// create object
	auto object = merger_create(self, config);

	// write object
	auto writer = merger_create_writer(self);
	merger_write(writer, config, &it->it, object, UINT64_MAX,
	             meta->hash_min,
	             meta->hash_max);
}

hot static void
merger_split(Merger* self, MergerConfig* config)
{
	auto meta = &config->origin->meta;

	// post refresh partition split by range
	auto first = merger_first(self);

	// prepare object iterator
	auto it = &self->object_iterator;
	object_iterator_reset(it);
	object_iterator_open(it, config->keys, first, NULL);

	// set partition file size limit
	auto limit = config->source->part_size / 2;

	// get writer
	auto writer = self->writers;
	while (object_iterator_has(it))
	{
		// create object
		auto object = merger_create(self, config);

		// write object
		writer_reset(writer);
		merger_write(writer, config, &it->it, object, limit,
		             meta->hash_min,
		             meta->hash_max);
	}
}

hot void
merger_execute(Merger* self, MergerConfig* config)
{
	auto source = config->source;

	// always refresh partition first
	//
	// if partition is currently in memory do full snapshot
	if (config->origin->source->in_memory)
		merger_snapshot(self, config);
	else
		merger_refresh(self, config);

	if (! source->auto_partitioning)
		return;

	// delete after merge
	auto object = merger_first(self);
	if (! object)
		return;

	// split partition if it exceeds partition size threshold

	// created object fits partition size limit
	if (object->meta.size_total <= (uint64_t)source->part_size)
		return;

	// split object file into N objects
	merger_split(self, config);

	// drop first object (created by refresh)
	merger_drop(self, object);
}
