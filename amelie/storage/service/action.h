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

typedef struct Action      Action;
typedef struct ServicePage ServicePage;

typedef enum
{
	ACTION_RECORD,
	ACTION_CHECKPOINT,
	ACTION_SYNC
} ActionType;

struct Action
{
	uint64_t   id;
	ActionType type;
	Uuid       db;
	Complete*  complete;
	uint32_t   data_size;
	uint8_t    data[];
};

static inline void
action_complete(Action* self)
{
	// notify group completion
	if (self->complete)
		complete_signal(self->complete);
}
