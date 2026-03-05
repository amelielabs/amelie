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

typedef struct ServiceReq ServiceReq;

struct ServiceReq
{
	Uuid id_table;
	int  refs;
	int  pending;
	List link;
};

static inline void
service_req_init(ServiceReq* self)
{
	self->refs = 0;
	self->pending = 0;
	uuid_init(&self->id_table);
	list_init(&self->link);
}

static inline ServiceReq*
service_req_allocate(void)
{
	auto self = (ServiceReq*)am_malloc(sizeof(ServiceReq));
	service_req_init(self);
	return self;
}

static inline void
service_req_free(ServiceReq* self)
{
	am_free(self);
}

static inline void
service_req_set(ServiceReq* self, Uuid* id_table)
{
	self->id_table = *id_table;
}
