#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct StorageMap StorageMap;

enum
{
	MAP_NONE,
	MAP_SHARD,
	MAP_SHARD_RANGE,
	MAP_SHARD_RANGE_AUTO,
};

struct StorageMap
{
	int64_t type;
	int64_t interval;
};

static inline StorageMap*
storage_map_allocate(void)
{
	StorageMap* self;
	self = mn_malloc(sizeof(StorageMap));
	self->type = MAP_NONE;
	self->interval = 0;
	return self;
}

static inline void
storage_map_free(StorageMap* self)
{
	mn_free(self);
}

static inline void
storage_map_set_type(StorageMap* self, int type)
{
	self->type = type;
}

static inline void
storage_map_set_interval(StorageMap* self, int interval)
{
	self->interval = interval;
}

static inline StorageMap*
storage_map_copy(StorageMap* self)
{
	auto copy = storage_map_allocate();
	guard(copy_guard, storage_map_free, copy);
	storage_map_set_type(copy, self->type);
	storage_map_set_interval(copy, self->interval);
	return unguard(&copy_guard);
}

static inline StorageMap*
storage_map_read(uint8_t** pos)
{
	auto self = storage_map_allocate();
	guard(self_guard, storage_map_free, self);

	// map
	int count;
	data_read_map(pos, &count);

	// type
	data_skip(pos);
	data_read_integer(pos, &self->type);

	// interval
	data_skip(pos);
	data_read_integer(pos, &self->interval);
	return unguard(&self_guard);
}

static inline void
storage_map_write(StorageMap* self, Buf* buf)
{
	// map
	encode_map(buf, 2);

	// type
	encode_raw(buf, "type", 4);
	encode_integer(buf, self->type);

	// interval
	encode_raw(buf, "interval", 8);
	encode_integer(buf, self->interval);
}
