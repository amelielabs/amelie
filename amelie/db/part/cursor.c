
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
#include <amelie_transaction.h>
#include <amelie_storage.h>
#include <amelie_heap.h>
#include <amelie_index.h>
#include <amelie_part.h>

hot static Iterator*
cursor_lookup(PartMgr*     self,
              Part*        part,
              IndexConfig* config,
              Row*         key)
{
	unused(self);
	auto index = part_index_find(part, &config->name, true);

	// check heap first
	auto it = index_iterator(index);
	errdefer(iterator_close, it);
	iterator_open(it, key);
	return it;
}

hot static Iterator*
cursor_scan(PartMgr*     self,
            Part*        part,
            IndexConfig* config,
            Row*         key)
{
	unused(self);
	auto index = part_index_find(part, &config->name, true);
	auto it = index_iterator(index);
	iterator_open(it, key);
	return it;
}

hot static Iterator*
cursor_scan_cross(PartMgr*     self,
                  IndexConfig* config,
                  Row*         key)
{
	// prepare heap merge iterators per partition
	Iterator* it = NULL;
	list_foreach(&self->list)
	{
		auto part = list_at(Part, link);
		auto index = part_index_find(part, &config->name, true);
		it = index_iterator_merge(index, it);
	}
	iterator_open(it, key);
	return it;
}

hot Iterator*
cursor_open(PartMgr*     self,
            Part*        part,
            IndexConfig* config,
            bool         point_lookup,
            Row*         key)
{
	// partition query
	if (part)
	{
		// point lookup
		if (point_lookup)
			return cursor_lookup(self, part, config, key);

		// range scan
		return cursor_scan(self, part, config, key);
	}

	// cross-partition query

	// point lookup (tree or hash index)
	if (point_lookup)
	{
		part = part_mapping_map(&self->mapping, key);
		return cursor_lookup(self, part, config, key);
	}

	// range scan

	// merge all hash partitions (without key)
	// merge all tree partitions (without key, ordered)
	// merge all tree partitions (with key, ordered)
	return cursor_scan_cross(self, config, key);
}
