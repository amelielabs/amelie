#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct ShardMap ShardMap;

struct ShardMap
{
	Shard** map;
	int     map_size;
};

static inline void
shard_map_init(ShardMap* self)
{
	self->map = NULL;
	self->map_size = 0;
}

static inline void
shard_map_free(ShardMap* self)
{
	if (self->map)
		mn_free(self->map);
}

static inline void
shard_map_create(ShardMap* self, ShardMgr* mgr)
{
	int allocated = sizeof(Shard*) * PARTITION_MAX;
	self->map = mn_malloc(allocated);
	self->map_size = PARTITION_MAX;
	for (int i = 0; i < mgr->shards_count; i++)
	{
		auto shard = mgr->shards[i];
		int j = shard->config->range_start;
		for (; j < shard->config->range_end; j++)
			self->map[j] = shard;
	}
}

hot static inline Shard*
shard_map_get(ShardMap* self, uint32_t hash)
{
	int partition = hash % PARTITION_MAX;
	return self->map[partition];
}
