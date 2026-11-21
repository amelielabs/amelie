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

typedef struct Action Action;

struct Action
{
	uint64_t lsn;
	Uuid     filter;
	uint32_t page;
	uint32_t data_size;
	uint8_t  data[];
} packed;

static inline uint32_t
action_size(Action* self)
{
	return sizeof(Action) + self->data_size;
}

static inline void
action_init(Action* self, Uuid* filter)
{
	self->lsn       = 0;
	self->filter    = *filter;
	self->page      = 0;
	self->data_size = 0;
}
