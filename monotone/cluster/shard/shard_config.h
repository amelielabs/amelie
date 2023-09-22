#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct ShardConfig ShardConfig;

struct ShardConfig
{
	Str     name;
	int64_t range_start;
	int64_t range_end;
};

static inline ShardConfig*
shard_config_allocate(void)
{
	ShardConfig* self;
	self = mn_malloc(sizeof(*self));
	self->range_start = 0;
	self->range_start = 0;
	str_init(&self->name);
	return self;
}

static inline void
shard_config_free(ShardConfig* self)
{
	mn_free(self);
}

static inline void
shard_config_set_name(ShardConfig* self, Str* name)
{
	str_copy(&self->name, name);
}

static inline void
shard_config_set_range(ShardConfig* self, int start, int end)
{
	self->range_start = start;
	self->range_end   = end;
}

static inline ShardConfig*
shard_config_copy(ShardConfig* self)
{
	auto copy = shard_config_allocate();
	guard(copy_guard, shard_config_free, copy);
	shard_config_set_name(copy, &self->name);
	shard_config_set_range(copy, self->range_start, self->range_end);
	return unguard(&copy_guard);
}

static inline ShardConfig*
shard_config_read(uint8_t** pos)
{
	auto self = shard_config_allocate();
	guard(self_guard, shard_config_free, self);
	// map
	int count;
	data_read_map(pos, &count);
	// name
	data_skip(pos);
	data_read_string_copy(pos, &self->name);
	// range_start
	data_skip(pos);
	int64_t integer;
	data_read_integer(pos, &integer);
	self->range_start = integer;
	// range_end
	data_skip(pos);
	data_read_integer(pos, &integer);
	self->range_end = integer;
	return unguard(&self_guard);
}

static inline void
shard_config_write(ShardConfig* self, Buf* buf)
{
	encode_map(buf, 3);
	encode_raw(buf, "name", 4);
	encode_string(buf, &self->name);
	encode_raw(buf, "range_start", 11);
	encode_integer(buf, self->range_start);
	encode_raw(buf, "range_end", 9);
	encode_integer(buf, self->range_end);
}
