#pragma once

//
// sonata.
//
// Real-Time SQL Database.
//

typedef struct Part Part;

struct Part
{
	List        indexes;
	int         indexes_count;
	Route*      route;
	PartConfig* config;
	List        link_cp;
	List        link;
};

Part*
part_allocate(PartConfig*);
void part_free(Part*);
void part_open(Part*, List*);
void part_ingest(Part*, uint8_t**);
void part_set(Part*, Transaction*, bool, uint8_t**);
void part_update(Part*, Transaction*, Iterator*, uint8_t**);
void part_delete(Part*, Transaction*, Iterator*);
void part_delete_by(Part*, Transaction*, uint8_t**);
void part_upsert(Part*, Transaction*, Iterator**, uint8_t**);

Index*
part_find(Part*, Str*, bool);

static inline Index*
part_primary(Part* self)
{
	return container_of(self->indexes.next, Index, link);
}
