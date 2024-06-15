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

// ddl
void part_create_index(Part*, IndexConfig*);
void part_drop_index(Part*, IndexConfig*);
Index*
part_find(Part*, Str*, bool);

// dml
void part_ingest(Part*, uint8_t**);
void part_set(Part*, Transaction*, bool, uint8_t**);
void part_update(Part*, Transaction*, Iterator*, uint8_t**);
void part_delete(Part*, Transaction*, Iterator*);
void part_delete_by(Part*, Transaction*, uint8_t**);
void part_upsert(Part*, Transaction*, Iterator**, uint8_t**);

static inline Index*
part_primary(Part* self)
{
	return container_of(self->indexes.next, Index, link);
}
