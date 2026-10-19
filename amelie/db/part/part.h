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
	Index*   indexes;
	int      indexes_count;
	Index*   in_progress;
	Heap*    heap;
	Flats    flats;
	PartArg* arg;
	void*    streams;
	List     link_cp;
	List     link;
};

Part*  part_allocate(PartArg*);
void   part_free(Part*);
void   part_open(Part*, Manifest*);
void   part_truncate(Part*);
void   part_column_create(Part*, Column*, uint32_t);
void   part_index_add(Part*, Index*);
void   part_index_create(Part*, IndexConfig*);
void   part_index_drop(Part*, Str*);
Index* part_index_find(Part*, Str*, bool);

static inline Index*
part_primary(Part* self)
{
	return self->indexes;
}
