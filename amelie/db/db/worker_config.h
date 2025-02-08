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

typedef struct WorkerConfig WorkerConfig;

struct WorkerConfig
{
	Str id;
	Str type;
};

static inline WorkerConfig*
worker_config_allocate(void)
{
	WorkerConfig* self;
	self = am_malloc(sizeof(*self));
	str_init(&self->id);
	str_init(&self->type);
	return self;
}

static inline void
worker_config_free(WorkerConfig* self)
{
	str_free(&self->id);
	str_free(&self->type);
	am_free(self);
}

static inline void
worker_config_set_id(WorkerConfig* self, Str* id)
{
	str_free(&self->id);
	str_copy(&self->id, id);
}

static inline void
worker_config_set_type(WorkerConfig* self, Str* type)
{
	str_free(&self->type);
	str_copy(&self->type, type);
}

static inline WorkerConfig*
worker_config_copy(WorkerConfig* self)
{
	auto copy = worker_config_allocate();
	worker_config_set_id(copy, &self->id);
	worker_config_set_type(copy, &self->type);
	return copy;
}

static inline WorkerConfig*
worker_config_read(uint8_t** pos)
{
	auto self = worker_config_allocate();
	errdefer(worker_config_free, self);
	Decode obj[] =
	{
		{ DECODE_STRING, "id",   &self->id   },
		{ DECODE_STRING, "type", &self->type },
		{ 0,              NULL,  NULL        },
	};
	decode_obj(obj, "worker", pos);
	return self;
}

static inline void
worker_config_write(WorkerConfig* self, Buf* buf)
{
	// obj
	encode_obj(buf);
	
	// id
	encode_raw(buf, "id", 2);
	encode_string(buf, &self->id);

	// type
	encode_raw(buf, "type", 4);
	encode_string(buf, &self->type);

	encode_obj_end(buf);
}
