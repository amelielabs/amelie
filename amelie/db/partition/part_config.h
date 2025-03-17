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

typedef struct PartConfig PartConfig;

struct PartConfig
{
	int64_t id;
	int64_t min;
	int64_t max;
	List    link;
};

static inline PartConfig*
part_config_allocate(void)
{
	PartConfig* self;
	self = am_malloc(sizeof(PartConfig));
	self->id  = 0;
	self->min = 0;
	self->max = 0;
	list_init(&self->link);
	return self;
}

static inline void
part_config_free(PartConfig* self)
{
	am_free(self);
}

static inline void
part_config_set_id(PartConfig* self, uint64_t id)
{
	self->id = id;
}

static inline void
part_config_set_range(PartConfig* self, int min, int max)
{
	self->min = min;
	self->max = max;
}

static inline PartConfig*
part_config_copy(PartConfig* self)
{
	auto copy = part_config_allocate();
	part_config_set_id(copy, self->id);
	part_config_set_range(copy, self->min, self->max);
	return copy;
}

static inline PartConfig*
part_config_read(uint8_t** pos)
{
	auto self = part_config_allocate();
	errdefer(part_config_free, self);
	Decode obj[] =
	{
		{ DECODE_INT, "id",  &self->id  },
		{ DECODE_INT, "min", &self->min },
		{ DECODE_INT, "max", &self->max },
		{ 0,           NULL,  NULL      },
	};
	decode_obj(obj, "part", pos);
	return self;
}

static inline void
part_config_write(PartConfig* self, Buf* buf)
{
	// obj
	encode_obj(buf);

	// id
	encode_raw(buf, "id", 2);
	encode_integer(buf, self->id);

	// min
	encode_raw(buf, "min", 3);
	encode_integer(buf, self->min);

	// max
	encode_raw(buf, "max", 3);
	encode_integer(buf, self->max);

	encode_obj_end(buf);
}
