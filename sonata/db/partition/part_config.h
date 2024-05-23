#pragma once

//
// sonata.
//
// Real-Time SQL Database.
//

typedef struct PartConfig PartConfig;

struct PartConfig
{
	int64_t id;
	Uuid    shard;
	int64_t shard_min;
	int64_t shard_max;
	List    link;
};

static inline PartConfig*
part_config_allocate(void)
{
	PartConfig* self;
	self = so_malloc(sizeof(PartConfig));
	self->id        = 0;
	self->shard_min = 0;
	self->shard_max = 0;
	uuid_init(&self->shard);
	list_init(&self->link);
	return self;
}

static inline void
part_config_free(PartConfig* self)
{
	so_free(self);
}

static inline void
part_config_set_id(PartConfig* self, uint64_t id)
{
	self->id = id;
}

static inline void
part_config_set_shard(PartConfig* self, Uuid* id)
{
	self->shard = *id;
}

static inline void
part_config_set_range(PartConfig* self, int min, int max)
{
	self->shard_min = min;
	self->shard_max = max;
}

static inline PartConfig*
part_config_copy(PartConfig* self)
{
	auto copy = part_config_allocate();
	guard(part_config_free, copy);
	part_config_set_id(copy, self->id);
	part_config_set_shard(copy, &self->shard);
	part_config_set_range(copy, self->shard_min, self->shard_max);
	return unguard();
}

static inline PartConfig*
part_config_read(uint8_t** pos)
{
	auto self = part_config_allocate();
	guard(part_config_free, self);
	Decode map[] =
	{
		{ DECODE_INT,  "id",        &self->id        },
		{ DECODE_UUID, "shard",     &self->shard     },
		{ DECODE_INT,  "shard_min", &self->shard_min },
		{ DECODE_INT,  "shard_max", &self->shard_max },
		{ 0,            NULL,       NULL             },
	};
	decode_map(map, pos);
	return unguard();
}

static inline void
part_config_write(PartConfig* self, Buf* buf)
{
	// map
	encode_map(buf, 4);

	// id
	encode_raw(buf, "id", 2);
	encode_integer(buf, self->id);

	// shard
	encode_raw(buf, "shard", 5);
	char uuid[UUID_SZ];
	uuid_to_string(&self->shard, uuid, sizeof(uuid));
	encode_raw(buf, uuid, sizeof(uuid) - 1);

	// shard_min
	encode_raw(buf, "shard_min", 9);
	encode_integer(buf, self->shard_min);

	// shard_max
	encode_raw(buf, "shard_max", 9);
	encode_integer(buf, self->shard_max);
}
