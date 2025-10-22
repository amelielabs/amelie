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

typedef struct QueryMgr QueryMgr;

struct QueryMgr
{
	List list;
};

static inline void
query_mgr_init(QueryMgr* self)
{
	list_init(&self->list);
}

static inline void
query_mgr_free(QueryMgr* self)
{
	list_foreach_safe(&self->list)
	{
		auto ql = list_at(Query, link);
		query_free(ql);
	}
}

static inline void
query_mgr_add(QueryMgr* self, Local* local, QueryIf* iface, Str* content_type)
{
	auto ql = query_allocate(iface, local, content_type);
	list_append(&self->list, &ql->link);
}

hot static inline Query*
query_mgr_find(QueryMgr* self, Str* content_type)
{
	list_foreach(&self->list)
	{
		auto ql = list_at(Query, link);
		if (str_compare(&ql->content_type, content_type))
			return ql;
	}
	return NULL;
}
