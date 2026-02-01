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
	Id        id;
	Index*    indexes;
	int       indexes_count;
	Track     track;
	Heap*     heap;
	Heap*     heap_shadow;
	Sequence* seq;
	bool      unlogged;
	List      link;
};

Part*  part_allocate(Id*, Sequence*, bool);
void   part_free(Part*);
void   part_open(Part*);
void   part_truncate(Part*);
void   part_index_add(Part*, IndexConfig*);
void   part_index_drop(Part*, Str*);
Index* part_index_find(Part*, Str*, bool);
void   part_status(Part*, Buf*, bool);

static inline Index*
part_primary(Part* self)
{
	return self->indexes;
}

static inline bool
part_has_updates(Part* self)
{
	return track_lsn(&self->track) > self->heap->header->lsn;
}
