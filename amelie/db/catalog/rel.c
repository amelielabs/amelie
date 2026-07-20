
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
#include <amelie_type.h>
#include <amelie_storage.h>
#include <amelie_flat.h>
#include <amelie_heap.h>
#include <amelie_cdc.h>
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_part.h>
#include <amelie_catalog.h>

void
catalog_drop_of(Catalog* self, Tr* tr, Rel* rel)
{
	// (no checks)
	rels_drop(&self->rels, tr, rel);
}

bool
catalog_drop(Catalog* self, Tr* tr, RelType type, Str* user, Str* name,
             bool     if_exists,
             bool     cascade)
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

	// collect dependencies
	Buf deps;
	buf_init(&deps);
	defer_buf(&deps);
	auto count = catalog_deps(self, rel, &deps);
	if (count > 0)
	{
		// cascade drop
		if (! cascade)
		{
			auto first = *(Rel**)deps.start;
			if (count >= 2)
				error("{s} '{str}': depends on {s} '{str}.{str}' and {d} more",
				      rel_type_of(type), name,
				      rel_type_of(first->type), first->user, first->name,
				      count - 1);
			else
				error("{s} '{str}': depends on {s} '{str}.{str}'",
				      rel_type_of(type), name,
				      rel_type_of(first->type), first->user, first->name);
		}
		catalog_deps_drop(self, tr, &deps);
	}

	// force commit pending prepared transactions
	if (type == REL_CLONE)
		table_sync(clone_of(rel)->table);

	// self
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
	rels_rename(&catalog->rels, op->rel, &user, &name);
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
	rels_rename(&self->rels, rel, user_new, name_new);
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

	// ensure no strict dependecies
	catalog_deps_validate(self, rel, true);

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

void
catalog_grant_of(Catalog* self,
                 Tr*      tr,
                 Rel*     rel,
                 Str*     to,
                 bool     grant,
                 uint32_t perms)
{
	// (no check)
	unused(self);

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
}

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

	// ensure user exists
	catalog_find_user(self, to, true);

	// validate permissions
	uint32_t perms_all = 0;
	switch (rel->type) {
	case REL_TABLE:
		perms_all =
			PERM_SELECT | PERM_INSERT | PERM_UPDATE |
			PERM_DELETE | PERM_CREATE_CLONE;
		break;
	case REL_CLONE:
		perms_all =
			PERM_SELECT | PERM_INSERT | PERM_UPDATE |
			PERM_DELETE | PERM_CREATE_CLONE;
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

	// grant/revoke
	catalog_grant_of(self, tr, rel, to, grant, perms);
	return true;
}

void
catalog_grant_rename_of(Catalog* self,
                        Tr*      tr,
                        Rel*     rel,
                        Str*     user,
                        Str*     user_new)
{
	// (no check)
	unused(self);

	// update table
	log_ddl(&tr->log, &grant_if, NULL, rel);

	// save previous grants
	auto grants = rel->grants;
	grants_write(grants, &tr->log.data, 0);

	// rename and replace grants
	Grants grants_new;
	grants_init(&grants_new);
	defer(grants_free, &grants_new);
	grants_copy_rename(&grants_new, grants, user, user_new);
	grants_reset(grants);
	grants_copy(grants, &grants_new);
}

static void
describe_if_commit(Log* self, LogOp* op)
{
	unused(self);
	unused(op);
}

static void
describe_if_abort(Log* self, LogOp* op)
{
	uint8_t* pos = log_data_of(self, op);
	Str description;
	unpack_str(&pos, &description);
	str_free(op->rel->description);
	str_copy(op->rel->description, &description);
}

static LogIf describe_if =
{
	.commit = describe_if_commit,
	.abort  = describe_if_abort
};

void
catalog_describe_of(Catalog* self,
                    Tr*      tr,
                    Rel*     rel,
                    Str*     description)
{
	// (no check)
	unused(self);
	assert(rel->description);

	// update table
	log_ddl(&tr->log, &describe_if, NULL, rel);

	// save previous description
	encode_str(&tr->log.data, rel->description);

	// set new description
	str_free(rel->description);
	str_copy(rel->description, description);
}

bool
catalog_describe(Catalog* self,
                 Tr*      tr,
                 RelType  type,
                 Str*     user,
                 Str*     name,
                 Str*     description,
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

	if (! rel->description)
		error("{s} '{str}': does not support description",
		      rel_type_of(type), name);

	catalog_describe_of(self, tr, rel, description);
	return true;
}
