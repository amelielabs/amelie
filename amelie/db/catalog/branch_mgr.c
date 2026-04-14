
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
#include <amelie_cdc.h>
#include <amelie_storage.h>
#include <amelie_heap.h>
#include <amelie_index.h>
#include <amelie_part.h>
#include <amelie_catalog.h>

void
branch_mgr_init(BranchMgr* self, TableMgr* table_mgr)
{
	self->table_mgr = table_mgr;
	rel_mgr_init(&self->mgr);
}

void
branch_mgr_free(BranchMgr* self)
{
	rel_mgr_free(&self->mgr);
}

bool
branch_mgr_create(BranchMgr*    self,
                  Tr*           tr,
                  BranchConfig* config,
                  bool          if_not_exists)
{
	// make sure branch does not exists
	auto branch = branch_mgr_find(self, &config->user, &config->name, false);
	if (branch)
	{
		if (! if_not_exists)
			error("branch '%.*s': already exists", str_size(&config->name),
			      str_of(&config->name));
		return false;
	}

	// ensure table exists
	auto table = table_mgr_find(self->table_mgr, &config->table_user,
	                            &config->table, true);

	// ensure permission to create branch on the table
	check_permission(tr, &table->rel, PERM_CREATE_BRANCH);

	// find parent branch
	auto parent = snapshot_mgr_find(&table->snapshot_mgr, config->snapshot.id_parent);
	if (! parent)
		error("branch '%.*s': parent branch cannot be found", str_size(&config->name),
		      str_of(&config->name));

	// create branch
	branch = branch_allocate(config);
	branch->table = table;
	branch->config->snapshot.parent = parent;
	str_set_str(&branch->config->snapshot.alias, &branch->config->name);
	rel_mgr_create(&self->mgr, tr, &branch->rel);

	// register branch snapshot
	snapshot_mgr_add(&table->snapshot_mgr, &branch->config->snapshot);
	return true;
}

void
branch_mgr_drop_of(BranchMgr* self, Tr* tr, Branch* branch)
{
	// drop branch by object
	rel_mgr_drop(&self->mgr, tr, &branch->rel);
}

bool
branch_mgr_drop(BranchMgr* self, Tr* tr, Str* user, Str* name,
                bool       if_exists)
{
	auto branch = branch_mgr_find(self, user, name, false);
	if (! branch)
	{
		if (! if_exists)
			error("branch '%.*s': not exists", str_size(name),
			      str_of(name));
		return false;
	}

	// only owner or superuser
	check_ownership(tr, &branch->rel);

	// ensure branch is not a parent branch
	list_foreach_safe(&self->mgr.list)
	{
		auto ref = list_at(Branch, rel.link);
		if (ref->table != branch->table)
			continue;
		if (ref->config->snapshot.id_parent == branch->config->snapshot.id)
			error("branch '%.*s': is a parent branch",
			      str_size(name), str_of(name));
	}

	branch_mgr_drop_of(self, tr, branch);
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
	Str user;
	Str name;
	json_read_string(&pos, &user);
	json_read_string(&pos, &name);

	auto branch = branch_of(op->rel);
	branch_config_set_user(branch->config, &user);
	branch_config_set_name(branch->config, &name);
}

static LogIf rename_if =
{
	.commit = rename_if_commit,
	.abort  = rename_if_abort
};

bool
branch_mgr_rename(BranchMgr* self,
                  Tr*        tr,
                  Str*       user,
                  Str*       name,
                  Str*       user_new,
                  Str*       name_new,
                  bool       if_exists)
{
	auto branch = branch_mgr_find(self, user, name, false);
	if (! branch)
	{
		if (! if_exists)
			error("branch '%.*s': not exists", str_size(name),
			      str_of(name));
		return false;
	}

	// only owner or superuser
	check_ownership(tr, &branch->rel);

	// ensure new branch does not exists
	if (branch_mgr_find(self, user_new, name_new, false))
		error("branch '%.*s': already exists", str_size(name_new),
		      str_of(name_new));

	// update branch
	log_ddl(&tr->log, &rename_if, NULL, &branch->rel);

	// save previous name
	encode_string(&tr->log.data, &branch->config->user);
	encode_string(&tr->log.data, &branch->config->name);

	// set new name
	if (! str_compare_case(&branch->config->user, user_new))
		branch_config_set_user(branch->config, user_new);

	if (! str_compare_case(&branch->config->name, name_new))
		branch_config_set_name(branch->config, name_new);

	return true;
}

static void
grant_if_commit(Log* self, LogOp* op)
{
	unused(self);
	unused(op);
}

static void
grant_if_abort(Log* self, LogOp* op)
{
	// restore grants
	uint8_t* pos = log_data_of(self, op);
	auto branch = branch_of(op->rel);
	auto grants = &branch->config->grants;
	grants_reset(grants);
	grants_read(grants, &pos);
}

static LogIf grant_if =
{
	.commit = grant_if_commit,
	.abort  = grant_if_abort
};

bool
branch_mgr_grant(BranchMgr* self,
                 Tr*        tr,
                 Str*       user,
                 Str*       name,
                 Str*       to,
                 bool       grant,
                 uint32_t   perms,
                 bool       if_exists)
{
	auto branch = branch_mgr_find(self, user, name, false);
	if (! branch)
	{
		if (! if_exists)
			error("branch '%.*s': not exists", str_size(name),
			      str_of(name));
		return false;
	}

	// only owner or superuser
	check_ownership(tr, &branch->rel);

	// validate permissions
	auto perms_all =
		 PERM_SELECT | PERM_INSERT | PERM_UPDATE |
		 PERM_DELETE | PERM_CREATE_BRANCH;
	perms = permission_validate(user, name, perms, perms_all);

	// update branch
	log_ddl(&tr->log, &grant_if, NULL, &branch->rel);

	// save previous grants
	auto grants = &branch->config->grants;
	grants_write(grants, &tr->log.data, 0);

	// set new grants
	if (grant)
		grants_add(grants, to, perms);
	else
		grants_remove(grants, to, perms);
	return true;
}

void
branch_mgr_dump(BranchMgr* self, Buf* buf)
{
	// array
	encode_array(buf);
	list_foreach(&self->mgr.list)
	{
		auto branch = branch_of(list_at(Rel, link));
		branch_config_write(branch->config, buf, 0);
	}
	encode_array_end(buf);
}

Branch*
branch_mgr_find(BranchMgr* self, Str* user, Str* name,
                bool       error_if_not_exists)
{
	auto rel = rel_mgr_get(&self->mgr, user, name);
	if (! rel)
	{
		if (error_if_not_exists)
			error("branch '%.*s': not exists", str_size(name),
			      str_of(name));
		return NULL;
	}
	return branch_of(rel);
}

Buf*
branch_mgr_list(BranchMgr* self, Str* user, Str* name, int flags)
{
	auto buf = buf_create();
	if (user && name)
	{
		// show branch
		auto branch = branch_mgr_find(self, user, name, false);
		if (branch)
			branch_config_write(branch->config, buf, flags);
		else
			encode_null(buf);
		return buf;
	}

	// show branches
	encode_array(buf);
	list_foreach(&self->mgr.list)
	{
		auto branch = branch_of(list_at(Rel, link));
		if (user && !str_compare_case(&branch->config->user, user))
			continue;
		branch_config_write(branch->config, buf, flags);
	}
	encode_array_end(buf);
	return buf;
}
