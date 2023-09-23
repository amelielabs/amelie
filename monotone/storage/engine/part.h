#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct Part Part;

struct Part
{
	bool     service;
	uint64_t id;
	uint64_t id_origin;
	int      range_start;
	int      range_end;
	Heap     heap;
	Schema*  schema;
	List     link_service;
	List     link;
};

static inline void
part_free(Part* self)
{
	heap_free(&self->heap);
	mn_free(self);
}

static inline Part*
part_allocate(Schema*  schema,
              Uuid*    uuid,
              uint64_t id_origin,
              uint64_t id,
              int      range_start,
              int      range_end)
{
	Part* self = mn_malloc(sizeof(Part));
	self->service     = false;
	self->id          = id;
	self->id_origin   = id_origin;
	self->range_start = range_start;
	self->range_end   = range_end;
	self->schema      = schema;
	list_init(&self->link_service);
	list_init(&self->link);
	heap_init(&self->heap, uuid, schema);

	guard(self_guard, part_free, self);
	heap_create(&self->heap, var_int_of(&config()->engine_partition));
	return unguard(&self_guard);
}
