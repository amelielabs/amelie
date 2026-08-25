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

typedef struct Affinity Affinity;

struct Affinity
{
	Str  name;
	Buf  cores;
	List link;
};

static inline Affinity*
affinity_allocate()
{
	Affinity* self;
	self = am_malloc(sizeof(Affinity));
	str_init(&self->name);
	buf_init(&self->cores);
	list_init(&self->link);
	return self;
}

static inline void
affinity_free(Affinity* self)
{
	str_free(&self->name);
	buf_free(&self->cores);
	am_free(self);
}

static inline void
affinity_set_name(Affinity* self, Str* value)
{
	str_free(&self->name);
	str_copy(&self->name, value);
}

static inline Affinity*
affinity_copy(Affinity* self)
{
	auto copy = affinity_allocate();
	affinity_set_name(copy, &self->name);
	buf_write_buf(&copy->cores, &self->cores);
	return copy;
}

static inline Affinity*
affinity_read(uint8_t** pos)
{
	auto self = affinity_allocate();
	errdefer(affinity_free, self);
	uint8_t* pos_cores = NULL;
	Decode obj[] =
	{
		{ DECODE_STR,   "name",  &self->name },
		{ DECODE_ARRAY, "cores", &pos_cores  },
		{ 0,             NULL,    NULL       },
	};
	decode_obj(obj, "affinity", pos);

	auto end = pos_cores;
	data_skip(&end);
	buf_write(&self->cores, pos_cores, end - pos_cores);
	return self;
}

static inline void
affinity_write(Affinity* self, Buf* buf, int flags)
{
	unused(flags);

	// obj
	encode_obj(buf);

	// name
	encode_raw(buf, "name", 4);
	encode_str(buf, &self->name);

	// cores
	encode_raw(buf, "cores", 5);
	buf_write_buf(buf, &self->cores);

	encode_obj_end(buf);
}
