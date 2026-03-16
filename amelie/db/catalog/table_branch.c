
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
#include <amelie_storage.h>
#include <amelie_object.h>
#include <amelie_heap.h>
#include <amelie_index.h>
#include <amelie_part.h>
#include <amelie_catalog.h>

static void
branch_create_if_commit(Log* self, LogOp* op)
{
	unused(self);
	unused(op);
}

static void
branch_create_if_abort(Log* self, LogOp* op)
{
	Table* table = op->iface_arg;
	auto rel = log_rel_of(self, op);
	uint8_t* pos = rel->data;
	Str branch_name;
	json_read_string(&pos, &branch_name);

	auto branches = &table->config->partitioning.branches;
	auto branch = branch_mgr_find(branches, &branch_name);
	assert(branch);
	branch_mgr_remove(branches, branch);
	branch_free(branch);
}

static LogIf branch_create_if =
{
	.commit = branch_create_if_commit,
	.abort  = branch_create_if_abort
};

bool
table_branch_create(Table*  self,
                    Tr*     tr,
                    Branch* config,
                    bool    if_not_exists)
{
	// ensure branch not exists
	auto branches = &self->config->partitioning.branches;
	auto branch = branch_mgr_find(branches, &config->name);
	if (branch)
	{
		if (! if_not_exists)
			error("table '%.*s' branch '%.*s': already exists",
			      str_size(&self->config->name),
			      str_of(&self->config->name),
			      str_size(&config->name),
			      str_of(&config->name));
		return false;
	}

	// update table
	log_rel(&tr->log, &branch_create_if, self, &self->rel);

	// save branch name
	encode_string(&tr->log.data, &config->name);

	// add branch
	branch = branch_copy(config);
	branch_mgr_add(branches, branch);
	return true;
}

static void
branch_drop_if_commit(Log* self, LogOp* op)
{
	Table* table = op->iface_arg;
	auto rel = log_rel_of(self, op);
	uint8_t* pos = rel->data;
	Str branch_name;
	json_read_string(&pos, &branch_name);

	auto branches = &table->config->partitioning.branches;
	auto branch = branch_mgr_find(branches, &branch_name);
	assert(branch);
	branch_mgr_remove(branches, branch);
	branch_free(branch);
}

static void
branch_drop_if_abort(Log* self, LogOp* op)
{
	unused(self);
	unused(op);
}

static LogIf branch_drop_if =
{
	.commit = branch_drop_if_commit,
	.abort  = branch_drop_if_abort
};

bool
table_branch_drop(Table* self,
                  Tr*    tr,
                  Str*   name,
                  bool   if_exists)
{
	// ensure branch exists
	auto branches = &self->config->partitioning.branches;
	auto branch = branch_mgr_find(branches, name);
	if (! branch)
	{
		if (! if_exists)
			error("table '%.*s' branch '%.*s': not found",
			      str_size(&self->config->name),
			      str_of(&self->config->name),
			      str_size(name),
			      str_of(name));
		return false;
	}

	// main branch cannot be dropped
	if (branch->id == 0)
		error("table '%.*s' branch '%.*s': cannot be dropped",
		      str_size(&self->config->name),
		      str_of(&self->config->name),
		      str_size(name),
		      str_of(name));

	// parent branch cannot be dropped
	if (branch_mgr_find_parent(branches, branch->id))
		error("table '%.*s' branch '%.*s': is a parent branch",
		      str_size(&self->config->name),
		      str_of(&self->config->name),
		      str_size(name),
		      str_of(name));

	// update table
	log_rel(&tr->log, &branch_drop_if, self, &self->rel);

	// save branch name
	encode_string(&tr->log.data, name);
	return true;
}
