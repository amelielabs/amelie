#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct ReqPartition ReqPartition;
typedef struct ReqMap       ReqMap;

#define PARTITION_MAX 8096

struct ReqPartition
{
	int      order;
	Channel* channel;
};

struct ReqMap
{
	ReqPartition* map;
	int           map_size;
	ReqPartition* map_order;
	int           map_order_size;
};

static inline void
req_map_init(ReqMap* self)
{
	self->map            = NULL;
	self->map_size       = 0;
	self->map_order      = NULL;
	self->map_order_size = 0;
}

static inline void
req_map_free(ReqMap* self)
{
	if (self->map)
		mn_free(self->map);
	if (self->map_order)
		mn_free(self->map_order);
}

static inline void
req_map_create(ReqMap* self, int count)
{
	int size = sizeof(ReqPartition) * PARTITION_MAX;
	self->map      = mn_malloc(size);
	self->map_size = PARTITION_MAX;
	memset(self->map, 0, size);

	size = sizeof(ReqPartition) * count;
	self->map_order = mn_malloc(size);
	self->map_order_size = count;
	memset(self->map_order, 0, size);
}

hot static inline ReqPartition*
req_map_at(ReqMap* self, int order)
{
	return &self->map_order[order];
}

hot static inline ReqPartition*
req_map_get(ReqMap* self, uint32_t hash)
{
	int partition = hash % PARTITION_MAX;
	return &self->map[partition];
}
