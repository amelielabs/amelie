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

typedef struct Backend Backend;

struct Backend
{
	Pods pods;
	Task task;
	List link;
};

Backend*
backend_allocate(void);
void backend_free(Backend*);
void backend_start(Backend*);
void backend_stop(Backend*);
