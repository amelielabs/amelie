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

typedef struct TierConfig TierConfig;

enum
{
	TIER_REGION_SIZE = 1 << 0,
	TIER_OBJECT_SIZE = 1 << 1
};

struct TierConfig
{
	Str       name;
	int64_t   region_size;
	int64_t   object_size;
	VolumeMgr volumes;
	List      link;
};

static inline void
tier_config_init(TierConfig* self)
{
	self->region_size = 64 * 1024;
	self->object_size = 64 * 1024 * 1024;
	volume_mgr_init(&self->volumes);
	str_init(&self->name);
	list_init(&self->link);
}

static inline TierConfig*
tier_config_allocate(void)
{
	auto self = (TierConfig*)am_malloc(sizeof(TierConfig));
	tier_config_init(self);
	return self;
}

static inline void
tier_config_free(TierConfig* self)
{
	str_free(&self->name);
	volume_mgr_free(&self->volumes);
	am_free(self);
}

static inline void
tier_config_set_name(TierConfig* self, Str* name)
{
	str_free(&self->name);
	str_copy(&self->name, name);
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

static inline TierConfig*
tier_config_copy(TierConfig* self)
{
	auto copy = tier_config_allocate();
	tier_config_set_name(copy, &self->name);
	tier_config_set_region_size(copy, self->region_size);
	tier_config_set_object_size(copy, self->object_size);
	volume_mgr_copy(&self->volumes, &copy->volumes);
	return copy;
}

static inline void
tier_config_set(TierConfig* self, TierConfig* alter, int mask)
{
	if ((mask & TIER_REGION_SIZE) > 0)
		tier_config_set_region_size(self, alter->region_size);

	if ((mask & TIER_OBJECT_SIZE) > 0)
		tier_config_set_object_size(self, alter->object_size);
}

static inline TierConfig*
tier_config_read(uint8_t** pos)
{
	auto self = tier_config_allocate();
	errdefer(tier_config_free, self);

	uint8_t* pos_volumes = NULL;
	Decode obj[] =
	{
		{ DECODE_STRING, "name",        &self->name        },
		{ DECODE_INT,    "region_size", &self->region_size },
		{ DECODE_INT,    "object_size", &self->object_size },
		{ DECODE_ARRAY,  "volumes",     &pos_volumes       },
		{ 0,              NULL,          NULL              },
	};
	decode_obj(obj, "tier_config", pos);

	// volumes
	volume_mgr_read(&self->volumes, &pos_volumes);
	return self;
}

static inline void
tier_config_write(TierConfig* self, Buf* buf, int flags)
{
	encode_obj(buf);

	// name
	encode_raw(buf, "name", 4);
	encode_string(buf, &self->name);

	// region_size
	encode_raw(buf, "region_size", 11);
	encode_integer(buf, self->region_size);

	// object_size
	encode_raw(buf, "object_size", 11);
	encode_integer(buf, self->object_size);

	// volumes
	encode_raw(buf, "volumes", 7);
	volume_mgr_write(&self->volumes, buf, flags);

	encode_obj_end(buf);
}
