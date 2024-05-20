#pragma once

//
// sonata.
//
// SQL Database for JSON.
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

static inline void
req_list_free(ReqList* self)
{
	list_foreach_safe(&self->list)
	{
		auto req = list_at(Req, link);
		req_free(req);
	}
}

static inline Req*
req_list_get(ReqList* self)
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
req_list_move(ReqList* self, ReqList* to, Req* req)
{
	list_unlink(&req->link);
	self->list_count--;
	req_list_add(to, req);
}
