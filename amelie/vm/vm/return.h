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

typedef struct Return Return;

struct Return
{
	Value* value;
};

static inline void
return_init(Return* self)
{
	self->value = NULL;
}
