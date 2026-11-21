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

typedef struct Service Service;

struct Service
{
	ServicePage* current;
	Buf          list;
	int          list_count;
	Action*      last;
	uint32_t     id;
	uint32_t     id_first;
	// subs
	Spinlock     subs_lock;
	List         subs;
	Subscriber*  waiters;
};

void service_init(Service*);
void service_free(Service*);
void service_attach(Service*, Subscriber*);
void service_add(Service*, Action*);
void service_resume(Service*);
Action*
service_next(Service*, Subscriber*);
