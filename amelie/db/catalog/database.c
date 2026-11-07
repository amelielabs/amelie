
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
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_part.h>
#include <amelie_catalog.h>

static void
database_sync_limits(Database* self)
{
	// memory
	auto limits = &self->config->limits;
	if (limits_is_set(limits, LIMIT_MEMORY))
		usage_set_limit(&self->memory, true, limits_get(limits, LIMIT_MEMORY));
	else
		usage_set_limit(&self->memory, false, 0);
}

static inline void
database_sync_revoke(Database* self)
{
	if (str_empty(&self->config->revoked_at))
	{
		self->inv_revoked = 0;
		return;
	}
	Timestamp ts;
	timestamp_init(&ts);
	timestamp_set(&ts, &self->config->revoked_at);
	auto time = timestamp_get_unixtime(&ts, runtime()->timezone);
	self->inv_revoked = time / 1000 / 1000;
}

static inline void
database_free(Database* self, bool drop)
{
	unused(drop);

	// undeploy
	self->databases->iface->detach(self->databases, self);

	catalog_free(&self->catalog);
	if (self->config)
		database_config_free(self->config);
	am_free(self);
}

static inline void
database_show(Database* self, Buf* buf, int flags)
{
	if (flags_has(flags, FCREATE))
		describe(&self->rel, buf, flags);
	else
		database_config_write(self->config, buf, flags);
}

static inline Database*
database_allocate(Catalog* databases, DatabaseConfig* config)
{
	Database* self = am_malloc(sizeof(Database));
	self->config      = database_config_copy(config);
	self->commit      = 0;
	self->inv_seq     = 0;
	self->inv_revoked = 0;
	self->compute     = NULL;
	self->databases   = databases;

	// prepare memory usage
	usage_init(&self->memory, "memory");
	database_sync_limits(self);

	// prepare catalog
	catalog_init(&self->catalog,
	             databases->iface,
	             databases->iface_arg,
	             &self->memory,
	             &self->config->name);

	// set relation
	auto rel = &self->rel;
	rel_init(rel, REL_DATABASE);
	rel_set_name(rel, &self->config->name);
	rel_set_description(rel, &self->config->description);
	rel_set_show(rel, (RelShow)database_show);
	rel_set_free(rel, (RelFree)database_free);
	return self;
}

bool
database_create(Catalog*        self,
                Tr*             tr,
                DatabaseConfig* config,
                bool            if_not_exists)
{
	// PERM_CREATE_DATABASE
	check_permission(tr, PERM_CREATE_DATABASE);

	// make sure database does not exists
	auto database = catalog_find_database(self, &config->name, false);
	if (database)
	{
		if (! if_not_exists)
			error("database '{str}': already exists", &config->name);
		return false;
	}

	// check limit
	catalog_limit(self, tr, REL_DATABASE, LIMIT_DATABASES);

	// create database
	database = database_allocate(self, config);
	rels_create(&self->rels, tr, &database->rel);

	// update timestamps
	database_sync_revoke(database);

	// deploy database
	self->iface->attach(self, database);
	return true;
}

static void
database_drop_of(Catalog* self,
                 Tr*      tr,
                 Rel*     database,
                 bool     cascade)
{
	// check deps
	list_foreach_safe(&self->rels.list)
	{
		auto ref = database_of(list_at(Rel, link));
		if (&ref->rel == database || !str_compare(&ref->config->parent, database->name))
			continue;
		if (cascade)
			database_drop_of(self, tr, &ref->rel, true);
		else
			error("database '{str}': has children", database->name);
	}

	// invalidate tokens
	database_invalidate(database_of(database));

	// drop database by object
	rels_drop(&self->rels, tr, database);
}

bool
database_drop(Catalog* self,
              Tr*      tr,
              Str*     name,
              bool     if_exists,
              bool     cascade)
{
	auto database = catalog_find_database(self, name, false);
	if (! database)
	{
		if (! if_exists)
			error("database '{str}': not exists", name);
		return false;
	}

	// only owner or system
	database_check_parent(tr, database);

	// system is immutable
	if (database->config->system)
		error("database '{str}': system database cannot be dropped", name);

	// drop database relations, database and child databases
	database_drop_of(self, tr, &database->rel, cascade);
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
	// set previous name
	uint8_t* pos = log_data_of(self, op);
	Str name;
	unpack_str(&pos, &name);

	Catalog* catalog = op->iface_arg;
	rels_rename(&catalog->rels, op->rel, &name);
}

static LogIf rename_if =
{
	.commit = rename_if_commit,
	.abort  = rename_if_abort
};

bool
database_rename(Catalog* self,
                Tr*      tr,
                Str*     name,
                Str*     name_new,
                bool     if_exists)
{
	auto database = catalog_find_database(self, name, false);
	if (! database)
	{
		if (! if_exists)
			error("database '{str}': not exists", name);
		return false;
	}

	// only owner or system
	database_check_parent(tr, database);

	// system is immutable
	if (database->config->system)
		error("database '{str}': system database cannot be renamed", name);

	// ensure new database does not exists
	if (catalog_find_database(self, name_new, false))
		error("database '{str}': already exists", name_new);

	// invalidate tokens
	database_invalidate(database);

	// update database
	log_ddl(&tr->log, &rename_if, self, &database->rel);

	// save name for rollback
	encode_str(&tr->log.data, name);

	// set new name
	rels_rename(&self->rels, &database->rel, name_new);
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
	auto database = database_of(op->rel);
	auto grants = &database->config->grants;
	grants_reset(grants);
	grants_read(grants, &pos);
}

static LogIf grant_if =
{
	.commit = grant_if_commit,
	.abort  = grant_if_abort
};

bool
database_grant(Catalog* self,
               Tr*      tr,
               Str*     name,
               bool     grant,
               uint32_t perms,
               bool     if_exists)
{
	auto database = catalog_find_database(self, name, false);
	if (! database)
	{
		if (! if_exists)
			error("database '{str}': not exists", name);
		return false;
	}

	// only owner or system
	database_check_parent(tr, database);

	// system is immutable
	if (database->config->system)
		error("database '{str}': system database cannot change grants", name);

#if 0
	// validate permissions
	auto perms_all =
		PERM_GRANT           |
		PERM_SYSTEM          |
		PERM_CREATE_DATABASE     |
		PERM_CREATE_TOKEN    |
		PERM_CREATE_TABLE    |
		PERM_CREATE_FUNCTION |
		PERM_CREATE_API      |
		PERM_SQL;
	perms = permission_validate(NULL, name, perms, perms_all);
#endif

	// PERM_SYSTEM for system wide grants
	if (((perms & PERM_SYSTEM) > 0) ||
	    ((perms & PERM_CREATE_DATABASE) > 0))
		check_permission(tr, PERM_SYSTEM);

	// update database
	log_ddl(&tr->log, &grant_if, NULL, &database->rel);

	// save previous grants
	auto grants = &database->config->grants;
	grants_write(grants, &tr->log.data, 0);

	// set new grants
	Str to;
	str_set(&to, "self", 4);
	if (grant)
		grants_add(grants, &to, perms);
	else
		grants_remove(grants, &to, perms);
	return true;
}

static void
revoke_token_if_commit(Log* self, LogOp* op)
{
	unused(self);
	unused(op);
}

static void
revoke_token_if_abort(Log* self, LogOp* op)
{
	// set previous revoked_at
	uint8_t* pos = log_data_of(self, op);
	Str value;
	unpack_str(&pos, &value);

	auto database = database_of(op->rel);
	database_config_set_revoked_at(database->config, &value);
	database_sync_revoke(database);
}

static LogIf revoke_token_if =
{
	.commit = revoke_token_if_commit,
	.abort  = revoke_token_if_abort
};

bool
database_revoke_token(Catalog* self,
                      Tr*      tr,
                      Str*     name,
                      Str*     revoked_at,
                      bool     if_exists)
{
	auto database = catalog_find_database(self, name, false);
	if (! database)
	{
		if (! if_exists)
			error("database '{str}': not exists", name);
		return false;
	}

	// only owner or system
	database_check_parent(tr, database);

	// system is immutable
	if (database->config->system)
		error("database '{str}': system database is immutable", name);

	// invalidate tokens
	database_invalidate(database);

	// update database
	log_ddl(&tr->log, &revoke_token_if, NULL, &database->rel);

	// save previous revoked_at for rollback
	encode_str(&tr->log.data, &database->config->revoked_at);

	// set revoked_at
	database_config_set_revoked_at(database->config, revoked_at);

	// update timestamps
	database_sync_revoke(database);
	return true;
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
	// set previous description
	uint8_t* pos = log_data_of(self, op);
	Str value;
	unpack_str(&pos, &value);

	auto database = database_of(op->rel);
	database_config_set_description(database->config, &value);
}

static LogIf describe_if =
{
	.commit = describe_if_commit,
	.abort  = describe_if_abort
};

bool
database_describe(Catalog* self,
                  Tr*      tr,
                  Str*     name,
                  Str*     description,
                  bool     if_exists)
{
	auto database = catalog_find_database(self, name, false);
	if (! database)
	{
		if (! if_exists)
			error("database '{str}': not exists", name);
		return false;
	}

	// only owner or system
	database_check_parent(tr, database);

	// system is immutable
	if (database->config->system)
		error("database '{str}': system database is immutable", name);

	// update database
	log_ddl(&tr->log, &describe_if, NULL, &database->rel);

	// save previous description
	encode_str(&tr->log.data, &database->config->description);

	// set new description
	database_config_set_description(database->config, description);
	return true;
}

static void
limit_set_if_commit(Log* self, LogOp* op)
{
	unused(self);
	unused(op);
}

static void
limit_set_if_abort(Log* self, LogOp* op)
{
	// set previous limits
	uint8_t* pos = log_data_of(self, op);
	Limits limits;
	limits_init(&limits);
	limits_read(&limits, &pos);

	auto database = database_of(op->rel);
	limits_copy(&database->config->limits, &limits);

	database_sync_limits(database);
}

static LogIf limit_set_if =
{
	.commit = limit_set_if_commit,
	.abort  = limit_set_if_abort
};

bool
database_limit_set(Catalog* self,
                   Tr*      tr,
                   Str*     name,
                   Limits*  limits,
                   bool     if_exists)
{
	auto database = catalog_find_database(self, name, false);
	if (! database)
	{
		if (! if_exists)
			error("database '{str}': not exists", name);
		return false;
	}

	// only owner or system
	database_check_parent(tr, database);

	// update database
	log_ddl(&tr->log, &limit_set_if, NULL, &database->rel);

	// save previous limits
	limits_write(&database->config->limits, &tr->log.data);

	// set new limits
	limits_copy(&database->config->limits, limits);

	database_sync_limits(database);
	return true;
}

bool
database_limit_unset(Catalog* self,
                     Tr*      tr,
                     Str*     name,
                     uint64_t mask,
                     bool     if_exists)
{
	auto database = catalog_find_database(self, name, false);
	if (! database)
	{
		if (! if_exists)
			error("database '{str}': not exists", name);
		return false;
	}

	// only owner or system
	database_check_parent(tr, database);

	// update database
	log_ddl(&tr->log, &limit_set_if, NULL, &database->rel);

	// save previous limits
	limits_write(&database->config->limits, &tr->log.data);

	// update limits
	database->config->limits.flags &= ~mask;

	database_sync_limits(database);
	return true;
}

static void
api_set_if_commit(Log* self, LogOp* op)
{
	unused(self);
	unused(op);
}

static void
api_set_if_abort(Log* self, LogOp* op)
{
	// restore previous apis
	auto database = database_of(op->rel);
	apis_free(&database->config->apis);

	uint8_t* pos = log_data_of(self, op);
	apis_read(&database->config->apis, &pos);
}

static LogIf api_set_if =
{
	.commit = api_set_if_commit,
	.abort  = api_set_if_abort
};

bool
database_api_create(Catalog* self,
                    Tr*      tr,
                    Str*     name,
                    Api*     api,
                    bool     if_exists)
{
	// PERM_CREATE_API
	check_permission(tr, PERM_CREATE_API);

	auto database = catalog_find_database(self, name, false);
	if (! database)
	{
		if (! if_exists)
			error("database '{str}': not exists", name);
		return false;
	}

	// only owner or system
	database_check_parent(tr, database);

	// ensure api is not redefined
	auto ref = apis_find(&database->config->apis, &api->uri);
	if (ref)
		error("database '{str}': api '{str}' already exists",
		      name, &api->uri);

	// update database
	log_ddl(&tr->log, &api_set_if, NULL, &database->rel);

	// save previous apis
	apis_write(&database->config->apis, &tr->log.data, 0);

	// add new api
	auto copy = api_copy(api);
	apis_add(&database->config->apis, copy);
	return true;
}

bool
database_api_drop(Catalog* self,
                  Tr*      tr,
                  Str*     name,
                  Str*     uri,
                  bool     if_exists)
{
	auto database = catalog_find_database(self, name, false);
	if (! database)
	{
		if (! if_exists)
			error("database '{str}': not exists", name);
		return false;
	}

	// only owner or system
	database_check_parent(tr, database);

	// ensure api is not redefined
	auto ref = apis_find(&database->config->apis, uri);
	if (! ref)
		error("database '{str}': api '{str}' not exists",
		      name, uri);

	// update database
	log_ddl(&tr->log, &api_set_if, NULL, &database->rel);

	// save previous apis
	apis_write(&database->config->apis, &tr->log.data, 0);

	// remove api
	apis_remove(&database->config->apis, ref);
	api_free(ref);
	return true;
}
