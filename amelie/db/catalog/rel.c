
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
catalog_drop_of(Catalog* self, Tr* tr, Rel* rel)
{
	// (no checks)
	rel_mgr_drop(&self->rels, tr, rel);
}

bool
catalog_drop(Catalog* self, Tr* tr, RelType type, Str* user, Str* name,
             bool     if_exists)
{
	auto rel = catalog_find(self, type, user, name, false);
	if (! rel)
	{
		if (! if_exists)
			error("{s} '{str}': not exists", rel_type_of(type), name);
		return false;
	}

	// only owner or superuser
	check_ownership(tr, rel);

	// ensure no udfs depend on the relation
	catalog_validate_udfs(self, user, name);

	// ensure branch is not a parent branch
	if (rel->type == REL_BRANCH)
	{
		auto branch = branch_of(rel);
		if (branch_is_parent(self, branch))
			error("branch '{str}': is a parent branch", name);
	}

	catalog_drop_of(self, tr, rel);
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

void
catalog_rename_of(Catalog* self,
                  Tr*      tr,
                  Rel*     rel,
                  Str*     user_new,
                  Str*     name_new)
{
	// (no checks)

	// update table
	log_ddl(&tr->log, &rename_if, self, rel);

	// save previous name
	encode_str(&tr->log.data, rel->user);
	encode_str(&tr->log.data, rel->name);

	// set new name
	rel_mgr_rename(&self->rels, rel, user_new, name_new);
}

bool
catalog_rename(Catalog* self,
               Tr*      tr,
               RelType  type,
               Str*     user,
               Str*     name,
               Str*     user_new,
               Str*     name_new,
               bool     if_exists)
{
	auto rel = catalog_find(self, type, user, name, false);
	if (! rel)
	{
		if (! if_exists)
			error("{s} '{str}': not exists", rel_type_of(type), name);
		return false;
	}

	// only owner or superuser
	check_ownership(tr, rel);

	// ensure user exists
	catalog_find_user(self, user_new, true);

	// ensure other relation with the same name does not exists
	if (catalog_find(self, REL_UNDEF, user_new, name_new, false))
		error("relation '{str}': already exists", name_new);

	// ensure no udfs depend on the relation
	catalog_validate_udfs(self, user, name);

	catalog_rename_of(self, tr, rel, user_new, name_new);
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
	auto grants = op->rel->grants;
	grants_reset(grants);
	grants_read(grants, &pos);
}

static LogIf grant_if =
{
	.commit = grant_if_commit,
	.abort  = grant_if_abort
};

bool
catalog_grant(Catalog* self,
              Tr*      tr,
              Str*     user,
              Str*     name,
              Str*     to,
              bool     grant,
              uint32_t perms)
{
	// PERM_GRANT
	check_user(tr, PERM_GRANT);

	// user grants
	if (str_empty(user))
	{
		// PERM_SYSTEM for system wide grants
		if (((perms & PERM_SYSTEM) > 0) ||
			((perms & PERM_CREATE_USER) > 0))
			check_user(tr, PERM_SYSTEM);
		return user_grant(self, tr, to, grant, perms, 0);
	}

	// find relation
	auto rel = catalog_find(self, REL_UNDEF, user, name, true);

	// only owner or superuser
	check_ownership(tr, rel);

	// validate permissions
	uint32_t perms_all = 0;
	switch (rel->type) {
	case REL_TABLE:
		perms_all =
			PERM_SELECT | PERM_INSERT | PERM_UPDATE |
			PERM_DELETE | PERM_CREATE_BRANCH;
		break;
	case REL_BRANCH:
		perms_all =
			PERM_SELECT | PERM_INSERT | PERM_UPDATE |
			PERM_DELETE | PERM_CREATE_BRANCH;
		break;
	case REL_UDF:
		perms_all = PERM_EXECUTE;
		break;
	case REL_TOPIC:
		perms_all = PERM_PUBLISH;
		break;
	case REL_SUBSCRIPTION:
		perms_all = PERM_NONE;
		break;
	default:
		abort();
		break;
	}
	perms = permission_validate(user, name, perms, perms_all);

	// update table
	log_ddl(&tr->log, &grant_if, NULL, rel);

	// save previous grants
	auto grants = rel->grants;
	grants_write(grants, &tr->log.data, 0);

	// set new grants
	if (grant)
		grants_add(grants, to, perms);
	else
		grants_remove(grants, to, perms);
	return true;
}
