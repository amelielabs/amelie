
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
	uint8_t* pos = log_data_of(self, op);
	Str branch_name;
	json_read_string(&pos, &branch_name);

	Table* table = op->iface_arg;
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

	// find parent
	auto parent = branch_mgr_find_by(branches, config->id_parent);
	if (! parent)
		error("table '%.*s' branch '%.*s': parent not found",
		      str_size(&self->config->name),
		      str_of(&self->config->name),
		      str_size(&config->name),
		      str_of(&config->name));

	// update table
	log_rel(&tr->log, &branch_create_if, self, &self->rel);

	// save branch name
	encode_string(&tr->log.data, &config->name);

	// add branch
	branch = branch_copy(config);
	branch->parent = parent;
	branch_mgr_add(branches, branch);
	return true;
}

static void
branch_drop_if_commit(Log* self, LogOp* op)
{
	uint8_t* pos = log_data_of(self, op);
	Str branch_name;
	json_read_string(&pos, &branch_name);

	Table* table = op->iface_arg;
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

static void
rename_if_commit(Log* self, LogOp* op)
{
	unused(self);
	unused(op);
}

static void
rename_if_abort(Log* self, LogOp* op)
{
	uint8_t* pos = log_data_of(self, op);
	Str branch_name;
	json_read_string(&pos, &branch_name);

	Branch* branch = op->iface_arg;
	branch_set_name(branch, &branch_name);
}

static LogIf rename_if =
{
	.commit = rename_if_commit,
	.abort  = rename_if_abort
};

bool
table_branch_rename(Table* self,
                    Tr*    tr,
                    Str*   name,
                    Str*   name_new,
                    bool   if_exists)
{
	auto branch = table_branch_find(self, name, false);
	if (! branch)
	{
		if (! if_exists)
			error("table '%.*s' branch '%.*s': not exists",
			      str_size(&self->config->name),
			      str_of(&self->config->name),
			      str_size(name),
			      str_of(name));
		return false;
	}

	// main branch cannot be renamed
	if (branch->id == 0)
		error("table '%.*s' branch '%.*s': cannot be renamed",
		      str_size(&self->config->name),
		      str_of(&self->config->name),
		      str_size(name),
		      str_of(name));

	// ensure new branch not exists
	if (table_branch_find(self, name_new, false))
		error("table '%.*s' branch '%.*s': already exists",
		      str_size(&self->config->name),
		      str_of(&self->config->name),
		      str_size(name_new),
		      str_of(name_new));

	// update table
	log_rel(&tr->log, &rename_if, branch, &self->rel);

	// save previous name
	encode_string(&tr->log.data, &branch->name);

	// rename branch
	branch_set_name(branch, name_new);
	return true;
}

hot Branch*
table_branch_find(Table* self, Str* name, bool error_if_not_exists)
{
	auto branch = branch_mgr_find(&self->config->partitioning.branches, name);

	if (!branch && error_if_not_exists)
		error("branch '%.*s': not exists", str_size(name),
		       str_of(name));
	return branch;
}

hot Branch*
table_branch_find_by(Table* self, uint32_t id, bool error_if_not_exists)
{
	auto branch = branch_mgr_find_by(&self->config->partitioning.branches, id);
	if (!branch && error_if_not_exists)
		error("branch %" PRIu32 ": not exists", id);
	return branch;
}

Buf*
table_branch_list(Table* self, Str* ref, int flags)
{
	auto buf = buf_create();
	errdefer_buf(buf);

	// show branch name on table
	if (ref)
	{
		auto branch = table_branch_find(self, ref, false);
		if (! branch)
			encode_null(buf);
		else
			branch_write(branch, buf, flags);
		return buf;
	}

	// show branches on table
	branch_mgr_write(&self->config->partitioning.branches, buf, flags);
	return buf;
}
