
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
	self->a = NULL;
	self->b = NULL;
	object_iterator_init(&self->object_iterator);
	heap_iterator_init(&self->heap_iterator);
	merge_iterator_init(&self->merge_iterator);
	writer_init(&self->writer);
}

void
merger_free(Merger* self)
{
	merger_reset(self);
	object_iterator_free(&self->object_iterator);
	merge_iterator_free(&self->merge_iterator);
	writer_free(&self->writer);
}

void
merger_reset(Merger* self)
{
	if (self->a)
	{
		object_free(self->a);
		self->a = NULL;
	}
	if (self->b)
	{
		object_free(self->b);
		self->b = NULL;
	}
	object_iterator_reset(&self->object_iterator);
	heap_iterator_reset(&self->heap_iterator);
	merge_iterator_reset(&self->merge_iterator);
	writer_reset(&self->writer);
}

hot static inline void
merger_write(Merger*   self, MergerConfig* config,
             Iterator* it,
             Object*   object,
             uint64_t  limit)
{
	auto origin = config->origin;
	auto heap   = config->heap;
	auto writer = &self->writer;

	writer_reset(writer);
	writer_start(writer, config->source, &object->file);
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
		if (writer_is_limit(writer, limit))
			break;
		iterator_next(it);
	}

	uint64_t lsn = heap->lsn_max;
	if (origin->meta.lsn > lsn)
		lsn = origin->meta.lsn;

	writer_stop(writer, &object->id, 0, lsn, config->source->sync);

	// copy and set meta data
	meta_writer_copy(&self->writer.meta_writer,
	                 &object->meta,
	                 &object->meta_data);
}

hot void
merger_merge(Merger* self, MergerConfig* config)
{
	auto origin = config->origin;

	// create zero, one or two objects by merging original object
	// with heap index

	// todo: use heap index

	// prepare heap iterator
	heap_iterator_open(&self->heap_iterator, config->heap, NULL);

	// prepare object iterator
	object_iterator_reset(&self->object_iterator);
	object_iterator_open(&self->object_iterator, config->keys, origin, NULL);

	// prepare merge iterator
	auto it = &self->merge_iterator;
	merge_iterator_reset(it);
	merge_iterator_add(it, &self->heap_iterator.it);
	merge_iterator_add(it, &self->object_iterator.it);
	merge_iterator_open(it, config->keys);

	// approximate stream size
	auto size = origin->meta.size_total_origin;
	size += page_mgr_used(&config->heap->page_mgr);

	uint64_t limit = config->source->part_size;
	if (size > limit)
		limit /= 2;

	// prepare id
	Id id =
	{
		.id        = 0,
		.id_parent = origin->id.id,
		.id_table  = origin->id.id_table
	};

	// create zero, one or two objects

	// left
	id.id = (*config->id_seq)++;
	auto left = object_allocate(config->source, &id);
	self->a = left;
	object_create(left, ID);
	object_set(left, ID);
	merger_write(self, config, &it->it, left, limit);
	if (! merge_iterator_has(it))
	{
		self->a = NULL;
		object_delete(left, ID);
		object_free(left);
		return;
	}

	// right
	id.id = (*config->id_seq)++;
	auto right = object_allocate(config->source, &id);
	self->b = right;
	object_create(right, ID);
	object_set(right, ID);
	merger_write(self, config, &it->it, right, UINT64_MAX);
}

hot static void
merger_snapshot(Merger* self, MergerConfig* config)
{
	auto origin = config->origin;

	// prepare heap iterator
	auto it = &self->heap_iterator;
	heap_iterator_open(it, config->heap, NULL);

	// create and write objects
	Id id;
	id.id        = (*config->id_seq)++;
	id.id_parent = origin->id.id;
	id.id_table  = origin->id.id_table;

	auto object = object_allocate(config->source, &id);
	self->a = object;
	object_create(object, ID);
	object_set(object, ID);

	// write file
	merger_write(self, config, &it->it, object, UINT64_MAX);
}

hot void
merger_execute(Merger* self, MergerConfig* config)
{
	switch (config->type) {
	case MERGER_SNAPSHOT:
		merger_snapshot(self, config);
		break;
	case MERGER_MERGE:
		merger_merge(self, config);
		break;
	}
}
