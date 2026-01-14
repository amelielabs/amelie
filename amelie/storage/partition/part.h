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

typedef struct Volume Volume;
typedef struct Part   Part;

struct Part
{
	Id         id;
	Source*    source;
	List       indexes;
	int        indexes_count;
	Track      track;
	Heap*      heap;
	Heap       heap_a;
	Heap       heap_b;
	Object*    object;
	Volume*    volume;
	Sequence*  seq;
	bool       unlogged;
	List       link;
	List       link_volume;
	RbtreeNode link_range;
};

Part*  part_allocate(Source*, Id*, Sequence*, bool);
void   part_free(Part*);
void   part_load(Part*);
void   part_truncate(Part*);
void   part_index_add(Part*, IndexConfig*);
void   part_index_drop(Part*, Str*);
Index* part_index_find(Part*, Str*, bool);

static inline void
part_set_volume(Part* self, Volume* volume)
{
	self->volume = volume;
}

static inline Index*
part_primary(Part* self)
{
	return container_of(self->indexes.next, Index, link);
}

static inline bool
part_has_updates(Part* self)
{
	return track_lsn(&self->track) > self->object->meta.lsn;
}
