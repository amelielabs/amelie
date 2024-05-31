#pragma once

//
// sonata.
//
// Real-Time SQL Database.
//

typedef struct ShardConfig ShardConfig;

struct ShardConfig
{
	Uuid id;
};

static inline ShardConfig*
shard_config_allocate(void)
{
	ShardConfig* self;
	self = so_malloc(sizeof(*self));
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

static inline ShardConfig*
shard_config_copy(ShardConfig* self)
{
	auto copy = shard_config_allocate();
	guard(shard_config_free, copy);
	shard_config_set_id(copy, &self->id);
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
		{ 0,            NULL,   NULL       }
	};
	decode_map(map, pos);
	return unguard();
}

static inline void
shard_config_write(ShardConfig* self, Buf* buf)
{
	encode_map(buf);

	// id
	encode_raw(buf, "id", 2);
	encode_uuid(buf, &self->id);

	encode_map_end(buf);
}
