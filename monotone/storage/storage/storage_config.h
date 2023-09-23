#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct StorageConfig StorageConfig;

struct StorageConfig
{
	Uuid    id;
	int64_t compression;
	bool    crc;
	Schema  schema;
};

static inline StorageConfig*
storage_config_allocate(void)
{
	StorageConfig *self;
	self = mn_malloc(sizeof(StorageConfig));
	self->compression = COMPRESSION_OFF;
	self->crc         = false;
	uuid_init(&self->id);
	schema_init(&self->schema);
	return self;
}

static inline void
storage_config_free(StorageConfig* self)
{
	schema_free(&self->schema);
	mn_free(self);
}

static inline void
storage_config_set_id(StorageConfig* self, Uuid* id)
{
	self->id = *id;
}

static inline void
storage_config_set_compression(StorageConfig* self, int id)
{
	self->compression = id;
}

static inline void
storage_config_set_crc(StorageConfig* self, bool crc)
{
	self->crc = crc;
}

static inline StorageConfig*
storage_config_copy(StorageConfig* self)
{
	auto copy = storage_config_allocate();
	guard(copy_guard, storage_config_free, copy);
	storage_config_set_id(copy, &self->id);
	storage_config_set_compression(copy, self->compression);
	storage_config_set_crc(copy, self->crc);
	schema_copy(&copy->schema, &self->schema);
	return unguard(&copy_guard);
}

static inline StorageConfig*
storage_config_read(uint8_t** pos)
{
	auto self = storage_config_allocate();
	guard(self_guard, storage_config_free, self);

	// map
	int count;
	data_read_map(pos, &count);

	// id
	Str id;
	data_skip(pos);
	data_read_string(pos, &id);
	uuid_from_string(&self->id, &id);

	// compression
	data_skip(pos);
	data_read_integer(pos, &self->compression);

	// crc
	data_skip(pos);
	data_read_bool(pos, &self->crc);

	// schema
	data_skip(pos);
	schema_read(&self->schema, pos);
	return unguard(&self_guard);
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

	// compression
	encode_raw(buf, "compression", 11);
	encode_integer(buf, self->compression);

	// crc
	encode_raw(buf, "crc", 3);
	encode_bool(buf, self->crc);

	// schema
	encode_raw(buf, "schema", 6);
	schema_write(&self->schema, buf);
}
