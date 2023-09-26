#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct ShardMgr ShardMgr;

struct ShardMgr
{
	Shard** shards;
	int     shards_count;
};

static inline void
shard_mgr_init(ShardMgr* self)
{
	self->shards       = NULL;
	self->shards_count = 0;
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

		auto shard = shard_allocate(config);
		self->shards[i] = shard;
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
	int range_max      = 8096;
	int range_interval = range_max / count;
	int range_start    = 0;

	for (int i = 0; i < count; i++)
	{
		// config
		auto config = shard_config_allocate();
		guard(config_guard, shard_config_free, config);

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

		// register
		auto shard = shard_allocate(config);
		self->shards[i] = shard;
	}

	shard_mgr_save(self);
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

static inline void
shard_mgr_assign(ShardMgr* self, StorageMgr* storage_mgr)
{
	// add associated storages per shard
	for (int i = 0; i < self->shards_count; i++)
	{
		auto shard = self->shards[i];
		storage_mgr_assign(storage_mgr, &shard->storage_list, &shard->config->id);
	}
}
