
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

static inline void
branch_free(Branch* self, bool drop)
{
	unused(drop);
	branch_config_free(self->config);
	am_free(self);
}

static inline void
branch_show(Branch* self, Buf* buf, int flags)
{
	branch_config_write(self->config, buf, flags);
}

static inline Branch*
branch_allocate(BranchConfig* config)
{
	auto self = (Branch*)am_malloc(sizeof(Branch));
	self->config = branch_config_copy(config);
	self->table  = NULL;

	// set relation
	auto rel = &self->rel;
	rel_init(rel, REL_BRANCH);
	rel_set_user(rel, &self->config->user);
	rel_set_name(rel, &self->config->name);
	rel_set_grants(rel, &self->config->grants);
	rel_set_show(rel, (RelShow)branch_show);
	rel_set_free(rel, (RelFree)branch_free);
	rel_set_rsn(rel, state_rsn_next());
	return self;
}

bool
branch_create(Catalog*      self,
              Tr*           tr,
              BranchConfig* config,
              bool          if_not_exists)
{
	// PERM_CREATE_BRANCH
	check_user(tr, PERM_CREATE_BRANCH);

	// make sure branch does not exists
	auto rel = catalog_find(self, REL_UNDEF, &config->user, &config->name, false);
	if (rel)
	{
		if (! if_not_exists)
			error("relation '{str}': already exists", &config->name);
		return false;
	}
	check_type(rel, REL_BRANCH);

	// ensure table exists
	auto table = catalog_find_table(self, &config->table_user, &config->table, true);

	// ensure permission to create branch on the table
	check_permission(tr, &table->rel, PERM_CREATE_BRANCH);

	// find parent branch
	auto parent = snapshot_mgr_find(&table->snapshot_mgr, config->snapshot.id_parent);
	if (! parent)
		error("branch '{str}': parent branch cannot be found", &config->name);

	// create branch
	auto branch = branch_allocate(config);
	branch->table = table;
	branch->config->snapshot.parent = parent;
	str_set_str(&branch->config->snapshot.alias, &branch->config->name);
	rel_mgr_create(&self->rels, tr, &branch->rel);

	// register branch snapshot
	snapshot_mgr_add(&table->snapshot_mgr, &branch->config->snapshot);
	return true;
}

void
branch_drop_of(Catalog* self, Tr* tr, Branch* branch)
{
	// drop branch by object
	rel_mgr_drop(&self->rels, tr, &branch->rel);
}

bool
branch_drop(Catalog* self, Tr* tr, Str* user, Str* name,
            bool     if_exists)
{
	auto branch = catalog_find_branch(self, user, name, false);
	if (! branch)
	{
		if (! if_exists)
			error("branch '{str}': not exists", name);
		return false;
	}

	// only owner or superuser
	check_ownership(tr, &branch->rel);

	// ensure branch is not a parent branch
	list_foreach_safe(&self->rels.list)
	{
		auto rel = list_at(Rel, link);
		if (rel->type != REL_BRANCH)
			continue;
		auto ref = branch_of(rel);
		if (ref->table != branch->table)
			continue;
		if (ref->config->snapshot.id_parent == branch->config->snapshot.id)
			error("branch '{str}': is a parent branch", name);
	}

	branch_drop_of(self, tr, branch);
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
	unpack_str(&pos, &user);
	unpack_str(&pos, &name);

	Catalog* catalog = op->iface_arg;
	rel_mgr_rename(&catalog->rels, op->rel, &user, &name);
}

static LogIf rename_if =
{
	.commit = rename_if_commit,
	.abort  = rename_if_abort
};

bool
branch_rename(Catalog* self,
              Tr*      tr,
              Str*     user,
              Str*     name,
              Str*     user_new,
              Str*     name_new,
              bool     if_exists)
{
	auto branch = catalog_find_branch(self, user, name, false);
	if (! branch)
	{
		if (! if_exists)
			error("branch '{str}': not exists", name);
		return false;
	}

	// only owner or superuser
	check_ownership(tr, &branch->rel);

	// ensure other relation with the same name does not exists
	if (catalog_find(self, REL_UNDEF, user_new, name_new, false))
		error("relation '{str}': already exists", name_new);

	// update branch
	log_ddl(&tr->log, &rename_if, NULL, &branch->rel);

	// save previous name
	encode_str(&tr->log.data, &branch->config->user);
	encode_str(&tr->log.data, &branch->config->name);

	// set new name
	rel_mgr_rename(&self->rels, &branch->rel, user_new, name_new);
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
branch_grant(Catalog* self,
             Tr*      tr,
             Str*     user,
             Str*     name,
             Str*     to,
             bool     grant,
             uint32_t perms,
             bool     if_exists)
{
	auto branch = catalog_find_branch(self, user, name, false);
	if (! branch)
	{
		if (! if_exists)
			error("branch '{name}': not exists", name);
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
