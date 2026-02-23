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

typedef struct PartMgrConfig PartMgrConfig;

struct PartMgrConfig
{
	Str       compression;
	int64_t   compression_level;
	int64_t   count;
	VolumeMgr volumes;
};

static inline void
part_mgr_config_init(PartMgrConfig* self)
{
	self->compression_level = 0;
	self->count             = 0;
	str_init(&self->compression);
	volume_mgr_init(&self->volumes);
}

static inline void
part_mgr_config_free(PartMgrConfig* self)
{
	str_free(&self->compression);
	volume_mgr_free(&self->volumes);
}

static inline void
part_mgr_config_set_compression(PartMgrConfig* self, Str* value)
{
	str_free(&self->compression);
	str_copy(&self->compression, value);
}

static inline void
part_mgr_config_set_compression_level(PartMgrConfig* self, int value)
{
	self->compression_level = value;
}

static inline void
part_mgr_config_set_count(PartMgrConfig* self, int value)
{
	self->count = value;
}

static inline void
part_mgr_config_copy(PartMgrConfig* self, PartMgrConfig* copy)
{
	part_mgr_config_set_compression(copy, &self->compression);
	part_mgr_config_set_compression_level(copy, self->compression_level);
	part_mgr_config_set_count(copy, self->count);
}

static inline void
part_mgr_config_read(PartMgrConfig* self, uint8_t** pos)
{
	uint8_t* pos_volumes = NULL;
	Decode obj[] =
	{
		{ DECODE_STRING, "compression",       &self->compression       },
		{ DECODE_INT,    "compression_level", &self->compression_level },
		{ DECODE_INT,    "count",             &self->count             },
		{ DECODE_ARRAY,  "volumes",           &pos_volumes             },
		{ 0,              NULL,                NULL                    },
	};
	decode_obj(obj, "part_mgr_config", pos);

	// volumes
	volume_mgr_read(&self->volumes, &pos_volumes);
}

static inline void
part_mgr_config_write(PartMgrConfig* self, Buf* buf, int flags)
{
	encode_obj(buf);

	// compression
	encode_raw(buf, "compression", 11);
	encode_string(buf, &self->compression);

	// compression_level
	encode_raw(buf, "compression_level", 17);
	encode_integer(buf, self->compression_level);

	// count
	encode_raw(buf, "count", 5);
	encode_integer(buf, self->count);

	// volumes
	encode_raw(buf, "volumes", 7);
	volume_mgr_write(&self->volumes, buf, flags);

	encode_obj_end(buf);
}
