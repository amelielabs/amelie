
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
#include <amelie_type.h>
#include <amelie_storage.h>
#include <amelie_flat.h>
#include <amelie_heap.h>
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_part.h>

hot static Iterator*
cursor_lookup(Part*        part,
              IndexConfig* config,
              Timeline*    timeline,
              Row*         key)
{
	assert(config);
	auto index = part_index_find(part, &config->name, true);

	// check heap first
	auto it = index_iterator(index);
	errdefer(iterator_close, it);
	iterator_open(it, timeline, key);
	return it;
}

hot static Iterator*
cursor_scan(Part*        part,
            IndexConfig* config,
            Timeline*    timeline,
            Row*         key)
{
	Iterator* it;
	if (! config)
	{
		// heap iterator
		it = iterator_heap_allocate(part->heap);
	} else
	{
		auto index = part_index_find(part, &config->name, true);
		it = index_iterator(index);
	}
	iterator_open(it, timeline, key);
	return it;
}

hot static Iterator*
cursor_scan_cross(Parts*       self,
                  IndexConfig* config,
                  Timeline*    timeline,
                  Row*         key)
{
	if (! config)
	{
		// todo: heap merge iterator
		abort();
		return NULL;
	}

	// prepare heap merge iterators per partition
	Iterator* it = NULL;
	list_foreach(&self->list)
	{
		auto part = list_at(Part, link);
		auto index = part_index_find(part, &config->name, true);
		it = index_iterator_merge(index, it);
	}

	// iterator use per partition heaps
	iterator_open(it, timeline, key);
	return it;
}

hot Iterator*
cursor_open(Parts*       self,
            Part*        part,
            IndexConfig* config,
            bool         point_lookup,
            Timeline*    timeline,
            Row*         key)
{
	// partition query
	if (part)
	{
		// point lookup
		if (point_lookup)
			return cursor_lookup(part, config, timeline, key);

		// range scan
		return cursor_scan(part, config, timeline, key);
	}

	// cross-partition query

	// point lookup (tree or hash index)
	if (point_lookup)
	{
		part = part_mapping_map(&self->mapping, key);
		return cursor_lookup(part, config, timeline, key);
	}

	// range scan

	// merge all hash partitions (without key)
	// merge all tree partitions (without key, ordered)
	// merge all tree partitions (with key, ordered)
	return cursor_scan_cross(self, config, timeline, key);
}
