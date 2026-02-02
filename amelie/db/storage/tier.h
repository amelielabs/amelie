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

typedef struct Tier  Tier;

struct Tier
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

static inline Tier*
tier_allocate(void)
{
	auto self = (Tier*)am_malloc(sizeof(Tier));
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
tier_free(Tier* self)
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
tier_set_name(Tier* self, Str* name)
{
	str_free(&self->name);
	str_copy(&self->name, name);
}

static inline void
tier_set_id(Tier* self, Uuid* id)
{
	self->id = *id;
}

static inline void
tier_set_compression(Tier* self, Str* value)
{
	str_free(&self->compression);
	str_copy(&self->compression, value);
}

static inline void
tier_set_compression_level(Tier* self, int value)
{
	self->compression_level = value;
}

static inline void
tier_set_encryption(Tier* self, Str* value)
{
	str_free(&self->encryption);
	str_copy(&self->encryption, value);
}

static inline void
tier_set_encryption_key(Tier* self, Str* value)
{
	str_free(&self->encryption_key);
	str_copy(&self->encryption_key, value);
}

static inline void
tier_set_region_size(Tier* self, int value)
{
	self->region_size = value;
}

static inline void
tier_set_object_size(Tier* self, int value)
{
	self->object_size = value;
}

static inline void
tier_set_system(Tier* self, bool value)
{
	self->system = value;
}

static inline void
tier_storage_add(Tier* self, TierStorage* storage)
{
	list_append(&self->storages, &storage->link);
	self->storages_count++;
}

static inline Tier*
tier_copy(Tier* self)
{
	auto copy = tier_allocate();
	tier_set_name(copy, &self->name);
	tier_set_id(copy, &self->id);
	tier_set_compression(copy, &self->compression);
	tier_set_compression_level(copy, self->compression_level);
	tier_set_encryption(copy, &self->encryption);
	tier_set_encryption_key(copy, &self->encryption_key);
	tier_set_region_size(copy, self->region_size);
	tier_set_object_size(copy, self->object_size);
	tier_set_system(copy, self->system);
	list_foreach(&self->storages)
	{
		auto storage = list_at(TierStorage, link);
		auto storage_copy = tier_storage_copy(storage);
		tier_storage_add(copy, storage_copy);
	}
	return copy;
}

static inline Tier*
tier_read(uint8_t** pos)
{
	auto self = tier_allocate();
	errdefer(tier_free, self);

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
	decode_obj(obj, "tier", pos);

	// storages
	json_read_array(&pos_storages);
	while (! json_read_array_end(&pos_storages))
	{
		auto storage = tier_storage_read(&pos_storages);
		tier_storage_add(self, storage);
	}
	return self;
}

static inline void
tier_write(Tier* self, Buf* buf, bool safe)
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

	if (safe)
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

static inline void
tier_ref(Tier* self, StorageMgr* storage_mgr)
{
	list_foreach(&self->storages)
	{
		auto ref = list_at(TierStorage, link);
		ref->storage = storage_mgr_find(storage_mgr, &ref->name, true);
		storage_ref(ref->storage);
	}
}

static inline void
tier_unref(Tier* self)
{
	list_foreach(&self->storages)
	{
		auto ref = list_at(TierStorage, link);
		if (ref->storage)
		{
			storage_unref(ref->storage);
			ref->storage = NULL;
		}
	}
}

static inline TierStorage*
tier_storage_find(Tier* self, Str* name)
{
	list_foreach(&self->storages)
	{
		auto ref = list_at(TierStorage, link);
		if (str_compare(&ref->name, name))
			return ref;
	}
	return NULL;
}

static inline TierStorage*
tier_storage(Tier* self)
{
	return container_of(list_first(&self->storages), TierStorage, link);
}
