#pragma once

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

hot static inline Iterator*
cursor_open(Part*        self,
            IndexConfig* config,
            bool         point_lookup,
            Timeline*    timeline,
            Row*         key)
{
	// point lookup
	Iterator* it;
	if (point_lookup)
	{
		assert(config);
		auto index = part_index_find(self, &config->name, true);

		// check heap first
		it = index_iterator(index);
		errdefer(iterator_close, it);
		iterator_open(it, timeline, key);
		return it;
	}

	// range scan
	if (! config)
	{
		// heap iterator
		it = iterator_heap_allocate(self->heap);
	} else
	{
		auto index = part_index_find(self, &config->name, true);
		it = index_iterator(index);
	}
	iterator_open(it, timeline, key);
	return it;
}
