#pragma once

//
// amelie.
//
// Real-Time SQL OLTP Database.
//
// Copyright (c) 2024 Dmitry Simonenko.
// AGPL-3.0 Licensed.
//

typedef struct Part Part;

struct Part
{
	List          indexes;
	int           indexes_count;
	Route*        route;
	Serial*       serial;
	PartConfig*   config;
	List          link_cp;
	List          link;
	HashtableNode link_ht;
};

Part*
part_allocate(PartConfig*, Serial*);
void part_free(Part*);
void part_index_create(Part*, IndexConfig*);
void part_index_drop(Part*, IndexConfig*);
void part_truncate(Part*);
Index*
part_find(Part*, Str*, bool);

static inline Index*
part_primary(Part* self)
{
	return container_of(self->indexes.next, Index, link);
}
