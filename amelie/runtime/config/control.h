#pragma once

//
// amelie.
//
// Real-Time SQL OLTP Database.
//
// Copyright (c) 2024 Dmitry Simonenko.
// AGPL-3.0 Licensed.
//

typedef struct Control Control;

struct Control
{
	Channel*  system;
	void    (*save_config)(void*);
	void*     arg;
};

static inline void
control_init(Control* self)
{
	memset(self, 0, sizeof(*self));
}
