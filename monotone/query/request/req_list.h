#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct ReqList ReqList;

struct ReqList
{
	List list;
	int  list_count;
};

static inline void
req_list_init(ReqList* self)
{
	self->list_count = 0;
	list_init(&self->list);
}

static inline void
req_list_add(ReqList* self, Req* req)
{
	list_append(&self->list, &req->link);
	self->list_count++;
}

static inline void
req_list_remove(ReqList* self, Req* req)
{
	list_unlink(&req->link);
	self->list_count--;
}
