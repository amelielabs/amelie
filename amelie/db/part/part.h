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
	Index*      indexes;
	int         indexes_count;
	Index*      in_progress;
	Track       track;
	Heap*       heap;
	PartConfig* config;
	PartArg*    arg;
	List        link_cp;
	List        link;
};

Part*  part_allocate(PartConfig*, PartArg*);
void   part_free(Part*);
void   part_open(Part*, uint64_t);
void   part_truncate(Part*);
void   part_index_add(Part*, Index*);
void   part_index_create(Part*, IndexConfig*);
void   part_index_drop(Part*, Str*);
Index* part_index_find(Part*, Str*, bool);
void   part_status(Part*, Buf*, int);

static inline Index*
part_primary(Part* self)
{
	return self->indexes;
}
