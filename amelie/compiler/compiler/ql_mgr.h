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

typedef struct QlMgr QlMgr;

struct QlMgr
{
	List list;
};

static inline void
ql_mgr_init(QlMgr* self)
{
	list_init(&self->list);
}

static inline void
ql_mgr_free(QlMgr* self)
{
	list_foreach_safe(&self->list)
	{
		auto ql = list_at(Ql, link);
		ql_free(ql);
	}
}

static inline void
ql_mgr_add(QlMgr* self, Local* local, QlIf* iface, Str* application_type)
{
	auto ql = ql_allocate(iface, local, application_type);
	list_append(&self->list, &ql->link);
}

hot static inline Ql*
ql_mgr_find(QlMgr* self, Str* application_type)
{
	list_foreach(&self->list)
	{
		auto ql = list_at(Ql, link);
		if (str_compare(&ql->application_type, application_type))
			return ql;
	}
	return NULL;
}
