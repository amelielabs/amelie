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
	Id            id;
	int           state;
	bool          refresh;
	// in-memory store
	List          indexes;
	int           indexes_count;
	Heap*         heap;
	Heap          heap_a;
	Heap          heap_b;
	Track         track;
	// file
	Span          span;
	Buf           span_data;
	File          file;
	Source*       source;
	List          link;
	HashtableNode link_mgr;
};

Part*  part_allocate(Source*, Id*);
void   part_free(Part*);
void   part_open(Part*, int, bool);
void   part_create(Part*, int);
void   part_delete(Part*, int);
void   part_rename(Part*, int, int);
void   part_offload(Part*, bool);
void   part_truncate(Part*);
void   part_add(Part*, IndexConfig*);
void   part_del(Part*, IndexConfig*);
Index* part_find(Part*, Str*, bool);
bool   part_ready(Part*);

static inline Index*
part_primary(Part* self)
{
	return container_of(self->indexes.next, Index, link);
}

static inline void
part_set(Part* self, int state)
{
	self->state |= state;
}

static inline void
part_unset(Part* self, int state)
{
	self->state &= ~state;
}

static inline bool
part_has(Part* self, int state)
{
	return (self->state & state) > 0;
}
