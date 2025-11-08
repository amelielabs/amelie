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
	int        r;
};

Plan* plan_create(Ast*, bool);
