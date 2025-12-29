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

typedef struct Part Part;

struct Part
{
	Track         track;
	List          indexes;
	int           indexes_count;
	Heap*         heap;
	Heap          heap_a;
	Heap          heap_b;
	Object*       object;
	Source*       source;
	List          link;
	HashtableNode link_mgr;
};

Part*  part_allocate(Source*);
void   part_free(Part*);
void   part_truncate(Part*);
void   part_index_add(Part*, IndexConfig*);
void   part_index_drop(Part*, Str*);
Index* part_index_find(Part*, Str*, bool);
bool   part_ready(Part*);

static inline Index*
part_primary(Part* self)
{
	return container_of(self->indexes.next, Index, link);
}
