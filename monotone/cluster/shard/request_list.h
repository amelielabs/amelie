#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct RequestList RequestList;

struct RequestList
{
	List list;
	int  list_count;
};

static inline void
request_list_init(RequestList* self)
{
	self->list_count = 0;
	list_init(&self->list);
}

static inline void
request_list_add(RequestList* self, Request* req)
{
	list_append(&self->list, &req->link);
	self->list_count++;
}

static inline void
request_list_remove(RequestList* self, Request* req)
{
	list_unlink(&req->link);
	self->list_count--;
}
