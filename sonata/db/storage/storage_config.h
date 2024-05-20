#pragma once

//
// sonata.
//
// SQL Database for JSON.
//

typedef struct StorageConfig StorageConfig;

struct StorageConfig
{
	Uuid    id;
	Uuid    shard;
	int64_t shard_min;
	int64_t shard_max;
	List    link;
};

static inline StorageConfig*
storage_config_allocate(void)
{
	StorageConfig* self;
	self = so_malloc(sizeof(StorageConfig));
	self->shard_min = 0;
	self->shard_max = 0;
	uuid_init(&self->id);
	uuid_init(&self->shard);
	list_init(&self->link);
	return self;
}

static inline void
storage_config_free(StorageConfig* self)
{
	so_free(self);
}

static inline void
storage_config_set_id(StorageConfig* self, Uuid* id)
{
	self->id = *id;
}

static inline void
storage_config_set_shard(StorageConfig* self, Uuid* id)
{
	self->shard = *id;
}

static inline void
storage_config_set_range(StorageConfig* self, int min, int max)
{
	self->shard_min = min;
	self->shard_max = max;
}

static inline StorageConfig*
storage_config_copy(StorageConfig* self)
{
	auto copy = storage_config_allocate();
	guard(storage_config_free, copy);
	storage_config_set_id(copy, &self->id);
	storage_config_set_shard(copy, &self->shard);
	storage_config_set_range(copy, self->shard_min, self->shard_max);
	return unguard();
}

static inline StorageConfig*
storage_config_read(uint8_t** pos)
{
	auto self = storage_config_allocate();
	guard(storage_config_free, self);
	Decode map[] =
	{
		{ DECODE_UUID, "id",        &self->id        },
		{ DECODE_UUID, "shard",     &self->shard     },
		{ DECODE_INT,  "shard_min", &self->shard_min },
		{ DECODE_INT,  "shard_max", &self->shard_max },
		{ 0,            NULL,       NULL             },
	};
	decode_map(map, pos);
	return unguard();
}

static inline void
storage_config_write(StorageConfig* self, Buf* buf)
{
	// map
	encode_map(buf, 4);

	// id
	encode_raw(buf, "id", 2);
	char uuid[UUID_SZ];
	uuid_to_string(&self->id, uuid, sizeof(uuid));
	encode_raw(buf, uuid, sizeof(uuid) - 1);

	// shard
	encode_raw(buf, "shard", 5);
	uuid_to_string(&self->shard, uuid, sizeof(uuid));
	encode_raw(buf, uuid, sizeof(uuid) - 1);

	// shard_min
	encode_raw(buf, "shard_min", 9);
	encode_integer(buf, self->shard_min);

	// shard_max
	encode_raw(buf, "shard_max", 9);
	encode_integer(buf, self->shard_max);
}
