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

typedef struct ReqList ReqList;

struct ReqList
{
	int  list_count;
	List list;
};

static inline void
req_list_init(ReqList* self)
{
	self->list_count = 0;
	list_init(&self->list);
}

hot static inline Req*
req_list_pop(ReqList* self)
{
	if (self->list_count == 0)
		return NULL;
	auto first = list_pop(&self->list);
	auto req = container_of(first, Req, link);
	self->list_count--;
	return req;
}

static inline void
req_list_add(ReqList* self, Req* req)
{
	list_append(&self->list, &req->link);
	self->list_count++;
}

static inline void
req_list_del(ReqList* self, Req* req)
{
	list_unlink(&req->link);
	self->list_count--;
}
