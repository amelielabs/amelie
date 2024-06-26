#pragma once

//
// sonata.
//
// Real-Time SQL Database.
//

typedef struct Part Part;

struct Part
{
	List          indexes;
	int           indexes_count;
	Route*        route;
	PartConfig*   config;
	List          link_cp;
	List          link;
	HashtableNode link_ht;
};

Part*
part_allocate(PartConfig*);
void part_free(Part*);
void part_index_create(Part*, IndexConfig*);
void part_index_drop(Part*, IndexConfig*);
Index*
part_find(Part*, Str*, bool);

static inline Index*
part_primary(Part* self)
{
	return container_of(self->indexes.next, Index, link);
}
