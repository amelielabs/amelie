#pragma once

//
// sonata.
//
// Real-Time SQL Database.
//

typedef struct ShardConfig ShardConfig;

struct ShardConfig
{
	Uuid    id;
	int64_t min;
	int64_t max;
};

static inline ShardConfig*
shard_config_allocate(void)
{
	ShardConfig* self;
	self = so_malloc(sizeof(*self));
	self->min = 0;
	self->max = 0;
	uuid_init(&self->id);
	return self;
}

static inline void
shard_config_free(ShardConfig* self)
{
	so_free(self);
}

static inline void
shard_config_set_id(ShardConfig* self, Uuid* id)
{
	self->id = *id;
}

static inline void
shard_config_set_range(ShardConfig* self, int min, int max)
{
	self->min = min;
	self->max = max;
}

static inline ShardConfig*
shard_config_copy(ShardConfig* self)
{
	auto copy = shard_config_allocate();
	guard(shard_config_free, copy);
	shard_config_set_id(copy, &self->id);
	shard_config_set_range(copy, self->min, self->max);
	return unguard();
}

static inline ShardConfig*
shard_config_read(uint8_t** pos)
{
	auto self = shard_config_allocate();
	guard(shard_config_free, self);
	Decode map[] =
	{
		{ DECODE_UUID, "id",   &self->id   },
		{ DECODE_INT,  "min",  &self->min  },
		{ DECODE_INT,  "max",  &self->max  },
		{ 0,            NULL,   NULL       }
	};
	decode_map(map, pos);
	return unguard();
}

static inline void
shard_config_write(ShardConfig* self, Buf* buf)
{
	encode_map(buf, 3);

	// id
	encode_raw(buf, "id", 2);
	char uuid[UUID_SZ];
	uuid_to_string(&self->id, uuid, sizeof(uuid));
	encode_raw(buf, uuid, sizeof(uuid) - 1);

	// min
	encode_raw(buf, "min", 3);
	encode_integer(buf, self->min);

	// max
	encode_raw(buf, "max", 3);
	encode_integer(buf, self->max);
}
