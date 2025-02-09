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
};

static inline WorkerConfig*
worker_config_allocate(void)
{
	WorkerConfig* self;
	self = am_malloc(sizeof(*self));
	str_init(&self->id);
	return self;
}

static inline void
worker_config_free(WorkerConfig* self)
{
	str_free(&self->id);
	am_free(self);
}

static inline void
worker_config_set_id(WorkerConfig* self, Str* id)
{
	str_free(&self->id);
	str_copy(&self->id, id);
}

static inline WorkerConfig*
worker_config_copy(WorkerConfig* self)
{
	auto copy = worker_config_allocate();
	worker_config_set_id(copy, &self->id);
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
	encode_obj_end(buf);
}
