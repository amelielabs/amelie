
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

#include <amelie_base.h>
#include <amelie_os.h>
#include <amelie_lib.h>
#include <amelie_json.h>
#include <amelie_runtime.h>
#include <amelie_row.h>
#include <amelie_heap.h>
#include <amelie_transaction.h>

void
relation_mgr_init(RelationMgr* self)
{
	self->list_count = 0;
	list_init(&self->list);
}

void
relation_mgr_free(RelationMgr* self)
{
	list_foreach_safe(&self->list)
	{
		auto relation = list_at(Relation, link);
		relation_free(relation);
	}
	self->list_count = 0;
	list_init(&self->list);
}

static inline void
relation_mgr_set(RelationMgr* self, Relation* relation)
{
	// previous version should not exists
	list_append(&self->list, &relation->link);
	self->list_count++;
}

static inline void
relation_mgr_delete(RelationMgr* self, Relation* relation)
{
	list_unlink(&relation->link);
	list_init(&relation->link);
	self->list_count--;
}

Relation*
relation_mgr_get(RelationMgr* self, Str* schema, Str* name)
{
	list_foreach(&self->list)
	{
		auto relation = list_at(Relation, link);
		if (schema && !str_compare_case(relation->schema, schema))
			continue;
		if (str_compare_case(relation->name, name))
			return relation;
	}
	return NULL;
}

void
relation_mgr_replace(RelationMgr* self, Relation* prev, Relation* relation)
{
	relation_mgr_delete(self, prev);
	relation_mgr_set(self, relation);
}

static void
create_if_commit(Log* self, LogOp* op)
{
	(void)self;
	(void)op;
}

static void
create_if_abort(Log* self, LogOp* op)
{
	auto relation = log_relation_of(self, op);
	RelationMgr* mgr = op->iface_arg;
	relation_mgr_delete(mgr, relation->relation);
	relation_free(relation->relation);
}

static LogIf create_if =
{
	.commit = create_if_commit,
	.abort  = create_if_abort
};

void
relation_mgr_create(RelationMgr* self,
                    Tr*          tr,
                    Relation*    relation)
{
	// update relation mgr
	relation_mgr_set(self, relation);

	// update transaction log
	log_relation(&tr->log, &create_if, self, relation);
}

static void
drop_if_commit(Log* self, LogOp* op)
{
	auto relation = log_relation_of(self, op);
	relation_free(relation->relation);
}

static void
drop_if_abort(Log* self, LogOp* op)
{
	auto relation = log_relation_of(self, op);
	RelationMgr* mgr = op->iface_arg;
	relation_mgr_set(mgr, relation->relation);
}

static LogIf drop_if =
{
	.commit = drop_if_commit,
	.abort  = drop_if_abort
};

void
relation_mgr_drop(RelationMgr* self,
                  Tr*          tr,
                  Relation*    relation)
{
	// update relation mgr
	relation_mgr_delete(self, relation);

	// update transaction log
	log_relation(&tr->log, &drop_if, self, relation);
}
