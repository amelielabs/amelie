#pragma once

//
// amelie.
//
// Real-Time SQL OLTP Database.
//
// Copyright (c) 2024 Dmitry Simonenko.
// Copyright (c) 2024 Amelie Labs.
//
// AGPL-3.0 Licensed.
//

typedef struct TierConfig  TierConfig;

struct TierConfig
{
	Str     name;
	Uuid    id;
	Str     compression;
	int64_t compression_level;
	Str     encryption;
	Str     encryption_key;
	int64_t region_size;
	int64_t object_size;
	List    storages;
	int     storages_count;
	bool    system;
	List    link;
};

static inline TierConfig*
tier_config_allocate(void)
{
	auto self = (TierConfig*)am_malloc(sizeof(TierConfig));
	self->compression_level = 0;
	self->region_size       = 64 * 1024;
	self->object_size       = 64 * 1024 * 1024;
	self->storages_count    = 0;
	self->system            = false;
	str_init(&self->name);
	uuid_init(&self->id);
	str_init(&self->compression);
	str_init(&self->encryption);
	str_init(&self->encryption_key);
	list_init(&self->storages);
	list_init(&self->link);
	return self;
}

static inline void
tier_config_free(TierConfig* self)
{
	str_free(&self->name);
	str_free(&self->compression);
	str_free(&self->encryption);
	str_free(&self->encryption_key);
	list_foreach_safe(&self->storages)
	{
		auto storage = list_at(TierStorage, link);
		tier_storage_free(storage);
	}
	am_free(self);
}

static inline void
tier_config_set_name(TierConfig* self, Str* name)
{
	str_free(&self->name);
	str_copy(&self->name, name);
}

static inline void
tier_config_set_id(TierConfig* self, Uuid* id)
{
	self->id = *id;
}

static inline void
tier_config_set_compression(TierConfig* self, Str* value)
{
	str_free(&self->compression);
	str_copy(&self->compression, value);
}

static inline void
tier_config_set_compression_level(TierConfig* self, int value)
{
	self->compression_level = value;
}

static inline void
tier_config_set_encryption(TierConfig* self, Str* value)
{
	str_free(&self->encryption);
	str_copy(&self->encryption, value);
}

static inline void
tier_config_set_encryption_key(TierConfig* self, Str* value)
{
	str_free(&self->encryption_key);
	str_copy(&self->encryption_key, value);
}

static inline void
tier_config_set_region_size(TierConfig* self, int value)
{
	self->region_size = value;
}

static inline void
tier_config_set_object_size(TierConfig* self, int value)
{
	self->object_size = value;
}

static inline void
tier_config_set_system(TierConfig* self, bool value)
{
	self->system = value;
}

static inline void
tier_config_storage_add(TierConfig* self, TierStorage* storage)
{
	list_append(&self->storages, &storage->link);
	self->storages_count++;
}

static inline TierConfig*
tier_config_copy(TierConfig* self)
{
	auto copy = tier_config_allocate();
	tier_config_set_name(copy, &self->name);
	tier_config_set_id(copy, &self->id);
	tier_config_set_compression(copy, &self->compression);
	tier_config_set_compression_level(copy, self->compression_level);
	tier_config_set_encryption(copy, &self->encryption);
	tier_config_set_encryption_key(copy, &self->encryption_key);
	tier_config_set_region_size(copy, self->region_size);
	tier_config_set_object_size(copy, self->object_size);
	tier_config_set_system(copy, self->system);
	list_foreach(&self->storages)
	{
		auto storage = list_at(TierStorage, link);
		auto storage_copy = tier_storage_copy(storage);
		tier_config_storage_add(copy, storage_copy);
	}
	return copy;
}

static inline TierConfig*
tier_config_read(uint8_t** pos)
{
	auto self = tier_config_allocate();
	errdefer(tier_config_free, self);

	uint8_t* pos_storages = NULL;
	Decode obj[] =
	{
		{ DECODE_STRING, "name",              &self->name              },
		{ DECODE_UUID,   "id",                &self->id                },
		{ DECODE_STRING, "compression",       &self->compression       },
		{ DECODE_INT,    "compression_level", &self->compression_level },
		{ DECODE_STRING, "encryption",        &self->encryption        },
		{ DECODE_STRING, "encryption_key",    &self->encryption_key    },
		{ DECODE_INT,    "region_size",       &self->region_size       },
		{ DECODE_INT,    "object_size",       &self->object_size       },
		{ DECODE_ARRAY,  "storages",          &pos_storages            },
		{ 0,              NULL,               NULL                     },
	};
	decode_obj(obj, "tier_config", pos);

	// storages
	json_read_array(&pos_storages);
	while (! json_read_array_end(&pos_storages))
	{
		auto storage = tier_storage_read(&pos_storages);
		tier_config_storage_add(self, storage);
	}
	return self;
}

static inline void
tier_config_write(TierConfig* self, Buf* buf, bool safe)
{
	encode_obj(buf);

	// name
	encode_raw(buf, "name", 4);
	encode_string(buf, &self->name);

	// id
	encode_raw(buf, "id", 2);
	encode_uuid(buf, &self->id);

	// compression
	encode_raw(buf, "compression", 11);
	encode_string(buf, &self->compression);

	// compression_level
	encode_raw(buf, "compression_level", 17);
	encode_integer(buf, self->compression_level);

	// encryption
	encode_raw(buf, "encryption", 10);
	encode_string(buf, &self->encryption);

	if (! safe)
	{
		// encryption_key
		encode_raw(buf, "encryption_key", 14);
		encode_string(buf, &self->encryption_key);
	}

	// region_size
	encode_raw(buf, "region_size", 11);
	encode_integer(buf, self->region_size);

	// object_size
	encode_raw(buf, "object_size", 11);
	encode_integer(buf, self->object_size);

	// storages
	encode_raw(buf, "storages", 8);
	encode_array(buf);
	list_foreach(&self->storages)
	{
		auto storage = list_at(TierStorage, link);
		tier_storage_write(storage, buf);
	}
	encode_array_end(buf);
	encode_obj_end(buf);
}
