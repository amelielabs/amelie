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

typedef struct Plan Plan;

struct Plan
{
	AstSelect* select;
	List       list;
	List       list_recv;
	bool       recv;
	int        r;
};

static inline void
plan_switch(Plan* self, bool recv)
{
	self->recv = recv;
}

Plan* plan_create(Ast*, bool);
