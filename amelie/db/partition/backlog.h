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

typedef struct BacklogReq BacklogReq;
typedef struct Backlog    Backlog;
typedef struct Part       Part;

struct BacklogReq
{
	List link;
};

struct Backlog
{
	List     list;
	int      list_count;
	TrList   prepared;
	TrCache  cache;
	Part*    part;
	bool     commit;
	Backlog* commit_next;
	List     link;
};

static inline void
backlog_req_init(BacklogReq* self)
{
	list_init(&self->link);
}

static inline void
backlog_init(Backlog* self, Part* part)
{
	self->list_count  = 0;
	self->commit      = false;
	self->commit_next = NULL;
	self->part        = part;
	list_init(&self->list);
	tr_list_init(&self->prepared);
	tr_cache_init(&self->cache);
	list_init(&self->link);
}

static inline void
backlog_free(Backlog* self)
{
	tr_cache_free(&self->cache);
}

static inline bool
backlog_add(Backlog* self, BacklogReq* req)
{
	auto is_first = !self->list_count;
	list_append(&self->list, &req->link);
	self->list_count++;
	return is_first;
}

hot static inline BacklogReq*
backlog_begin(Backlog* self)
{
	if (! self->list_count)
		return NULL;

	// take next pending request, keep it in the list till completion
	auto first = list_first(&self->list);
	auto req = container_of(first, BacklogReq, link);
	return req;
}

hot static inline bool
backlog_end(Backlog* self, BacklogReq* req)
{
	auto first = list_pop(&self->list);
	assert(first == &req->link);
	unused(first);
	unused(req);
	self->list_count--;
	return self->list_count > 0;
}
