#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct Part Part;

struct Part
{
	uint64_t   id;
	Uuid*      id_storage;
	Uuid*      id_table;
	uint64_t   min;
	uint64_t   max;
	List       indexes;
	int        indexes_count;
	RbtreeNode link_node;
	List       link;
};

Part*
part_allocate(Uuid*, Uuid*, uint64_t);
void part_free(Part*);
void part_open(Part*, List*);

void part_set(Part*, Transaction*, bool, uint8_t*, int);
void part_update(Part*, Transaction*, Iterator*, uint8_t*, int);
void part_delete(Part*, Transaction*, Iterator*);
void part_delete_by(Part*, Transaction*, uint8_t*, int);
bool part_upsert(Part*, Transaction*, Iterator**, uint8_t*, int);

Index*
part_find(Part*, Str*, bool);

static inline Index*
part_primary(Part* self)
{
	return container_of(self->indexes.next, Index, link);
}

static inline int
part_compare(Part* self, uint64_t value)
{
	if (self->min == value)
		return 0;
	return (self->min > value) ? 1 : -1;
}
