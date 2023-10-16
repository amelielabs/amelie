#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct ShardMgr ShardMgr;

struct ShardMgr
{
	Shard**      shards;
	int          shards_count;
	FunctionMgr* function_mgr;
	Db*          db;
};

static inline void
shard_mgr_init(ShardMgr* self, Db* db, FunctionMgr* function_mgr)
{
	self->shards       = NULL;
	self->shards_count = 0;
	self->function_mgr = function_mgr;
	self->db           = db;
}

static inline void
shard_mgr_free(ShardMgr* self)
{
	if (self->shards)
	{
		for (int i = 0; i < self->shards_count; i++)
		{
			auto shard = self->shards[i];
			if (shard) {
				shard_stop(shard);
				shard_free(shard);
			}
		}
		mn_free(self->shards);
	}
}

static inline Buf*
shard_mgr_dump(ShardMgr* self)
{
	auto dump = buf_create(0);
	// array
	encode_array(dump, self->shards_count);
	for (int i = 0; i < self->shards_count; i++)
	{
		auto shard = self->shards[i];
		shard_config_write(shard->config, dump);
	}
	return dump;
}

static inline void
shard_mgr_save(ShardMgr* self)
{
	// create dump
	auto dump = shard_mgr_dump(self);

	// update and save state
	var_data_set_buf(&config()->shards, dump);
	control_save_config();
	buf_free(dump);
}

static inline void
shard_mgr_open(ShardMgr* self)
{
	auto shards = &config()->shards;
	if (! var_data_is_set(shards))
		return;
	auto pos = var_data_of(shards);
	if (data_is_null(pos))
		return;

	int count;
	data_read_array(&pos, &count);

	int allocated = count * sizeof(Shard*);
	self->shards_count = count;
	self->shards = mn_malloc(allocated);
	memset(self->shards, 0, allocated);
	for (int i = 0; i < count; i++)
	{
		// value
		auto config = shard_config_read(&pos);
		guard(config_guard, shard_config_free, config);

		auto shard = shard_allocate(config, self->db, self->function_mgr);
		self->shards[i] = shard;

		// set shard order
		shard->order = i;
	}
}

static inline void
shard_mgr_create(ShardMgr* self, int count)
{
	// prepare index
	int allocated = count * sizeof(Shard*);
	self->shards_count = count;
	self->shards = mn_malloc(allocated);
	memset(self->shards, 0, allocated);

	// partition_max / shards_count
	int range_max      = PARTITION_MAX;
	int range_interval = range_max / count;
	int range_start    = 0;

	for (int i = 0; i < count; i++)
	{
		// config
		auto config = shard_config_allocate();
		guard(config_guard, shard_config_free, config);

		// set shard id
		Uuid id;
		uuid_mgr_generate(global()->uuid_mgr, &id);
		shard_config_set_id(config, &id);

		// set shard name
		char name_sz[32];
		snprintf(name_sz, sizeof(name_sz), "shard%d", i);
		Str name;
		str_set_cstr(&name, name_sz);
		shard_config_set_name(config, &name);

		// set shard range
		int range_step;
		if ((i + 1) < count)
			range_step = range_interval;
		else
			range_step = range_max - range_start;
		if ((range_start + range_step) > range_max)
			range_step = range_max - range_start;

		shard_config_set_range(config, range_start, range_start + range_step);

		range_start += range_step;

		// create shard
		auto shard = shard_allocate(config, self->db, self->function_mgr);
		self->shards[i] = shard;

		// set shard order
		shard->order = i;
	}

	shard_mgr_save(self);
}

static inline void
shard_mgr_set_partition_map(ShardMgr* mgr, ReqMap* map)
{
	// create partition map to shard
	req_map_create(map, mgr->shards_count);

	for (int i = 0; i < mgr->shards_count; i++)
	{
		auto shard = mgr->shards[i];
		auto partition = &map->map_order[i];
		partition->order   =  shard->order;
		partition->channel = &shard->task.channel;

		int j = shard->config->range_start;
		for (; j < shard->config->range_end; j++)
		{
			partition = &map->map[j];
			partition->order   =  shard->order;
			partition->channel = &shard->task.channel;
		}
	}
}

static inline void
shard_mgr_start(ShardMgr* self)
{
	for (int i = 0; i < self->shards_count; i++)
	{
		auto shard = self->shards[i];
		shard_start(shard);
	}
}

static inline void
shard_mgr_stop(ShardMgr* self)
{
	for (int i = 0; i < self->shards_count; i++)
	{
		auto shard = self->shards[i];
		shard_stop(shard);
	}
}
