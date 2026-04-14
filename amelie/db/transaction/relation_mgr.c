
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

#include <amelie_runtime>
#include <amelie_row.h>
#include <amelie_transaction.h>

void
rel_mgr_init(RelMgr* self)
{
	self->list_count = 0;
	list_init(&self->list);
}

void
rel_mgr_free(RelMgr* self)
{
	list_foreach_safe(&self->list)
	{
		auto rel = list_at(Rel, link);
		rel_free(rel);
	}
	self->list_count = 0;
	list_init(&self->list);
}

static inline void
rel_mgr_set(RelMgr* self, Rel* rel)
{
	// previous version should not exists
	list_append(&self->list, &rel->link);
	self->list_count++;
}

static inline void
rel_mgr_delete(RelMgr* self, Rel* rel)
{
	list_unlink(&rel->link);
	list_init(&rel->link);
	self->list_count--;
}

Rel*
rel_mgr_get(RelMgr* self, Str* user, Str* name)
{
	list_foreach(&self->list)
	{
		auto rel = list_at(Rel, link);
		if (user && !str_compare_case(rel->user, user))
			continue;
		if (str_compare_case(rel->name, name))
			return rel;
	}
	return NULL;
}

void
rel_mgr_replace(RelMgr* self, Rel* prev, Rel* rel)
{
	rel_mgr_delete(self, prev);
	rel_mgr_set(self, rel);
}

static void
create_if_commit(Log* self, LogOp* op)
{
	unused(self);
	unused(op);
}

static void
create_if_abort(Log* self, LogOp* op)
{
	unused(self);
	RelMgr* mgr = op->iface_arg;
	rel_mgr_delete(mgr, op->rel);
	rel_drop(op->rel);
}

static LogIf create_if =
{
	.commit = create_if_commit,
	.abort  = create_if_abort
};

void
rel_mgr_create(RelMgr* self, Tr* tr, Rel* rel)
{
	// update rel mgr
	rel_mgr_set(self, rel);

	// update transaction log
	log_ddl(&tr->log, &create_if, self, rel);
}

static void
drop_if_commit(Log* self, LogOp* op)
{
	unused(self);
	rel_drop(op->rel);
}

static void
drop_if_abort(Log* self, LogOp* op)
{
	unused(self);
	RelMgr* mgr = op->iface_arg;
	rel_mgr_set(mgr, op->rel);
}

static LogIf drop_if =
{
	.commit = drop_if_commit,
	.abort  = drop_if_abort
};

void
rel_mgr_drop(RelMgr* self, Tr* tr, Rel* rel)
{
	// update rel mgr
	rel_mgr_delete(self, rel);

	// update transaction log
	log_ddl(&tr->log, &drop_if, self, rel);
}
