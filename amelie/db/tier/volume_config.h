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

typedef struct VolumeConfig VolumeConfig;

struct VolumeConfig
{
	Str     tier;
	int64_t partitions;
	int64_t size;
	int64_t rows;
	int64_t duration;
	List    link;
};

static inline VolumeConfig*
volume_config_allocate(void)
{
	auto self = (VolumeConfig*)am_malloc(sizeof(VolumeConfig));
	self->partitions = -1;
	self->size       = -1;
	self->rows       = -1;
	self->duration   = -1;
	str_init(&self->tier);
	list_init(&self->link);
	return self;
}

static inline void
volume_config_free(VolumeConfig* self)
{
	str_free(&self->tier);
	am_free(self);
}

static inline void
volume_config_set_tier(VolumeConfig* self, Str* tier)
{
	str_free(&self->tier);
	str_copy(&self->tier, tier);
}

static inline void
volume_config_set_partitions(VolumeConfig* self, int64_t value)
{
	self->partitions = value;
}

static inline void
volume_config_set_size(VolumeConfig* self, int64_t value)
{
	self->size = value;
}

static inline void
volume_config_set_rows(VolumeConfig* self, int64_t value)
{
	self->rows = value;
}

static inline void
volume_config_set_duration(VolumeConfig* self, int64_t value)
{
	self->duration = value;
}

static inline VolumeConfig*
volume_config_copy(VolumeConfig* self)
{
	auto copy = volume_config_allocate();
	volume_config_set_tier(copy, &self->tier);
	volume_config_set_partitions(copy, self->partitions);
	volume_config_set_size(copy, self->size);
	volume_config_set_rows(copy, self->rows);
	volume_config_set_duration(copy, self->duration);
	return copy;
}

static inline VolumeConfig*
volume_config_read(uint8_t** pos)
{
	auto self = volume_config_allocate();
	errdefer(source_free, self);
	Decode obj[] =
	{
		{ DECODE_STRING, "tier",       &self->tier       },
		{ DECODE_INT,    "partitions", &self->partitions },
		{ DECODE_INT,    "size",       &self->size       },
		{ DECODE_INT,    "rows",       &self->rows       },
		{ DECODE_INT,    "duration",   &self->duration   },
		{ 0,              NULL,        NULL              },
	};
	decode_obj(obj, "volume_config", pos);
	return self;
}

static inline void
volume_config_write(VolumeConfig* self, Buf* buf)
{
	encode_obj(buf);

	// tier
	encode_raw(buf, "tier", 4);
	encode_string(buf, &self->tier);

	// partitions
	encode_raw(buf, "partitions", 10);
	encode_integer(buf, self->partitions);

	// size
	encode_raw(buf, "size", 4);
	encode_integer(buf, self->size);

	// rows
	encode_raw(buf, "rows", 4);
	encode_integer(buf, self->rows);

	// duration
	encode_raw(buf, "duration", 8);
	encode_integer(buf, self->duration);

	encode_obj_end(buf);
}

static inline bool
volume_config_terminal(VolumeConfig* self)
{
	return self->partitions == -1 &&
	       self->size       == -1 &&
	       self->rows       == -1 &&
	       self->duration   == -1;
}
