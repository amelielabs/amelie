#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct Part Part;

struct Part
{
	Uuid*      id_storage;
	Uuid*      id_table;
	int64_t    min;
	int64_t    max;
	uint64_t   snapshot;
	List       indexes;
	int        indexes_count;
	RbtreeNode link_node;
	List       link;
};

Part*
part_allocate(Uuid*, Uuid*);
void part_free(Part*);
void part_open(Part*, List*);
void part_snapshot(Part*, Snapshot*);

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
part_compare(Part* self, int64_t value)
{
	if (self->min == value)
		return 0;
	return (self->min > value) ? 1 : -1;
}
