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
	List      list;
	SetCache* set_cache;
	Local*    local;
};

static inline void
query_mgr_init(QueryMgr* self, Local* local, SetCache* set_cache)
{
	self->set_cache = set_cache;
	self->local     = local;
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
query_mgr_add(QueryMgr* self, QueryIf* iface, Str* content_type)
{
	auto ql = query_allocate(iface, self->local, self->set_cache, content_type);
	list_append(&self->list, &ql->link);
}

hot static inline Query*
query_mgr_find(QueryMgr* self, Str* content_type)
{
	list_foreach(&self->list)
	{
		auto ql = list_at(Query, link);
		if (str_compare_case(&ql->content_type, content_type))
			return ql;
	}
	return NULL;
}
