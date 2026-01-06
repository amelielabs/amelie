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

typedef struct MappingConfig MappingConfig;

enum
{
	MAPPING_RANGE,
	MAPPING_HASH
};

struct MappingConfig
{
	int64_t type;
	int64_t count;
};

static inline MappingConfig*
mapping_config_allocate(void)
{
	auto self = (MappingConfig*)am_malloc(sizeof(MappingConfig));
	self->type  = -1;
	self->count = -1;
	return self;
}

static inline void
mapping_config_free(MappingConfig* self)
{
	am_free(self);
}

static inline void
mapping_config_set_type(MappingConfig* self, int64_t value)
{
	self->type = value;
}

static inline void
mapping_config_set_count(MappingConfig* self, int64_t value)
{
	self->count = value;
}

static inline MappingConfig*
mapping_config_copy(MappingConfig* self)
{
	auto copy = mapping_config_allocate();
	mapping_config_set_type(copy, self->type);
	mapping_config_set_count(copy, self->count);
	return copy;
}

static inline MappingConfig*
mapping_config_read(uint8_t** pos)
{
	auto self = mapping_config_allocate();
	errdefer(source_free, self);
	Decode obj[] =
	{
		{ DECODE_INT, "type",  &self->type  },
		{ DECODE_INT, "count", &self->count },
		{ 0,           NULL,    NULL        },
	};
	decode_obj(obj, "mapping", pos);
	return self;
}

static inline void
mapping_config_write(MappingConfig* self, Buf* buf)
{
	encode_obj(buf);

	// type
	encode_raw(buf, "type", 4);
	encode_integer(buf, self->type);

	// count
	encode_raw(buf, "count", 5);
	encode_integer(buf, self->count);

	encode_obj_end(buf);
}
