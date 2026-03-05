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

typedef struct ServiceWork ServiceWork;

struct ServiceWork
{
	ServiceReq* req;
	int         pending;
	bool        worked;
	Event       event;
	List        link;
};

static inline void
service_work_init(ServiceWork* self)
{
	self->req     = NULL;
	self->pending = 0;
	self->worked  = false;
	event_init(&self->event);
	list_init(&self->link);
}
