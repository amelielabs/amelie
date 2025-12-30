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

typedef struct Space Space;
typedef struct Part  Part;

struct Part
{
	List          indexes;
	int           indexes_count;
	Track         track;
	Heap*         heap;
	Heap          heap_a;
	Heap          heap_b;
	PartConfig*   config;
	Sequence*     seq;
	bool          unlogged;
	Source*       source;
	Space*        space;
	List          link;
	List          link_space;
	HashtableNode link_mgr;
};

Part*  part_allocate(PartConfig*, Source*, Sequence*, bool);
void   part_free(Part*);
void   part_truncate(Part*);
void   part_index_add(Part*, IndexConfig*);
void   part_index_drop(Part*, Str*);
Index* part_index_find(Part*, Str*, bool);

static inline Index*
part_primary(Part* self)
{
	return container_of(self->indexes.next, Index, link);
}
