
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
#include <amelie_heap>
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_part.h>
#include <amelie_catalog.h>

static void
db_sync_limits(Db* self)
{
	// memory
	auto limits = &self->config->limits;
	if (limits_is_set(limits, LIMIT_MEMORY))
		usage_set_limit(&self->memory, true, limits_get(limits, LIMIT_MEMORY));
	else
		usage_set_limit(&self->memory, false, 0);
}

static inline void
db_sync_revoke(Db* self)
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
db_free(Db* self, bool drop)
{
	unused(drop);

	// undeploy
	self->databases->iface->detach(self->databases, self);

	catalog_free(&self->catalog);
	if (self->config)
		db_config_free(self->config);
	am_free(self);
}

static inline void
db_show(Db* self, Buf* buf, int flags)
{
	if (flags_has(flags, FCREATE))
		describe(&self->rel, buf, flags);
	else
		db_config_write(self->config, buf, flags);
}

static inline Db*
db_allocate(Catalog* databases, DbConfig* config)
{
	Db* self = am_malloc(sizeof(Db));
	self->config      = db_config_copy(config);
	self->inv_seq     = 0;
	self->inv_revoked = 0;
	self->commit      = 0;
	self->compute     = NULL;
	self->databases   = databases;

	// prepare memory usage
	usage_init(&self->memory, "memory");
	db_sync_limits(self);

	// prepare catalog
	catalog_init(&self->catalog,
	             databases->iface,
	             databases->iface_arg,
	             &self->memory,
	             &self->config->name);

	// set relation
	auto rel = &self->rel;
	rel_init(rel, REL_DB);
	rel_set_name(rel, &self->config->name);
	rel_set_description(rel, &self->config->description);
	rel_set_show(rel, (RelShow)db_show);
	rel_set_free(rel, (RelFree)db_free);
	return self;
}

bool
db_create(Catalog*  self,
          Tr*       tr,
          DbConfig* config,
          bool      if_not_exists)
{
	// PERM_CREATE_DATABASE
	check_permission(tr, PERM_CREATE_DATABASE);

	// make sure db does not exists
	auto db = catalog_find_db(self, &config->name, false);
	if (db)
	{
		if (! if_not_exists)
			error("db '{str}': already exists", &config->name);
		return false;
	}

	// create db
	db = db_allocate(self, config);
	rels_create(&self->rels, tr, &db->rel);

	// update timestamps
	db_sync_revoke(db);

	// deploy db
	self->iface->attach(self, db);
	return true;
}

static void
db_drop_of(Catalog* self,
           Tr*      tr,
           Rel*     db,
           bool     cascade)
{
	// check dependencies
	list_foreach_safe(&self->rels.list)
	{
		auto ref = db_of(list_at(Rel, link));
		if (&ref->rel == db || !str_compare(&ref->config->parent, db->name))
			continue;
		if (cascade)
			db_drop_of(self, tr, &ref->rel, true);
		else
			error("db '{str}': has children", db->name);
	}

	// invalidate tokens
	db_invalidate(db_of(db));

	// drop db by object
	rels_drop(&self->rels, tr, db);
}

bool
db_drop(Catalog* self,
        Tr*      tr,
        Str*     name,
        bool     if_exists,
        bool     cascade)
{
	auto db = catalog_find_db(self, name, false);
	if (! db)
	{
		if (! if_exists)
			error("db '{str}': not exists", name);
		return false;
	}

	// only owner or system
	db_check_ownership(tr, db);

	// system is immutable
	if (db->config->system)
		error("db '{str}': system db cannot be dropped", name);

	// drop db relations, db and child databases
	db_drop_of(self, tr, &db->rel, cascade);
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
db_rename(Catalog* self,
          Tr*      tr,
          Str*     name,
          Str*     name_new,
          bool     if_exists)
{
	auto db = catalog_find_db(self, name, false);
	if (! db)
	{
		if (! if_exists)
			error("db '{str}': not exists", name);
		return false;
	}

	// only owner or system
	db_check_ownership(tr, db);

	// system is immutable
	if (db->config->system)
		error("db '{str}': system db cannot be renamed", name);

	// ensure new db does not exists
	if (catalog_find_db(self, name_new, false))
		error("db '{str}': already exists", name_new);

	// invalidate tokens
	db_invalidate(db);

	// update db
	log_ddl(&tr->log, &rename_if, self, &db->rel);

	// save name for rollback
	encode_str(&tr->log.data, name);

	// set new name
	rels_rename(&self->rels, &db->rel, name_new);
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
	auto db = db_of(op->rel);
	auto grants = &db->config->grants;
	grants_reset(grants);
	grants_read(grants, &pos);
}

static LogIf grant_if =
{
	.commit = grant_if_commit,
	.abort  = grant_if_abort
};

bool
db_grant(Catalog* self,
         Tr*      tr,
         Str*     name,
         bool     grant,
         uint32_t perms,
         bool     if_exists)
{
	auto db = catalog_find_db(self, name, false);
	if (! db)
	{
		if (! if_exists)
			error("db '{str}': not exists", name);
		return false;
	}

	// only owner or system
	db_check_ownership(tr, db);

	// system is immutable
	if (db->config->system)
		error("db '{str}': system db cannot change grants", name);

#if 0
	// validate permissions
	auto perms_all =
		PERM_GRANT           |
		PERM_SYSTEM          |
		PERM_CREATE_DATABASE |
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

	// update db
	log_ddl(&tr->log, &grant_if, NULL, &db->rel);

	// save previous grants
	auto grants = &db->config->grants;
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

	auto db = db_of(op->rel);
	db_config_set_revoked_at(db->config, &value);
	db_sync_revoke(db);
}

static LogIf revoke_token_if =
{
	.commit = revoke_token_if_commit,
	.abort  = revoke_token_if_abort
};

bool
db_revoke_token(Catalog* self,
                Tr*      tr,
                Str*     name,
                Str*     revoked_at,
                bool     if_exists)
{
	auto db = catalog_find_db(self, name, false);
	if (! db)
	{
		if (! if_exists)
			error("db '{str}': not exists", name);
		return false;
	}

	// only owner or system
	db_check_ownership(tr, db);

	// system is immutable
	if (db->config->system)
		error("db '{str}': system db is immutable", name);

	// invalidate tokens
	db_invalidate(db);

	// update db
	log_ddl(&tr->log, &revoke_token_if, NULL, &db->rel);

	// save previous revoked_at for rollback
	encode_str(&tr->log.data, &db->config->revoked_at);

	// set revoked_at
	db_config_set_revoked_at(db->config, revoked_at);

	// update timestamps
	db_sync_revoke(db);
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

	auto db = db_of(op->rel);
	db_config_set_description(db->config, &value);
}

static LogIf describe_if =
{
	.commit = describe_if_commit,
	.abort  = describe_if_abort
};

bool
db_describe(Catalog* self,
            Tr*      tr,
            Str*     name,
            Str*     description,
            bool     if_exists)
{
	auto db = catalog_find_db(self, name, false);
	if (! db)
	{
		if (! if_exists)
			error("db '{str}': not exists", name);
		return false;
	}

	// only owner or system
	db_check_ownership(tr, db);

	// system is immutable
	if (db->config->system)
		error("db '{str}': system db is immutable", name);

	// update db
	log_ddl(&tr->log, &describe_if, NULL, &db->rel);

	// save previous description
	encode_str(&tr->log.data, &db->config->description);

	// set new description
	db_config_set_description(db->config, description);
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

	auto db = db_of(op->rel);
	limits_copy(&db->config->limits, &limits);

	db_sync_limits(db);
}

static LogIf limit_set_if =
{
	.commit = limit_set_if_commit,
	.abort  = limit_set_if_abort
};

bool
db_limit_set(Catalog* self,
             Tr*      tr,
             Str*     name,
             Limits*  limits,
             bool     if_exists)
{
	auto db = catalog_find_db(self, name, false);
	if (! db)
	{
		if (! if_exists)
			error("db '{str}': not exists", name);
		return false;
	}

	// only owner or system
	db_check_ownership(tr, db);

	// update db
	log_ddl(&tr->log, &limit_set_if, NULL, &db->rel);

	// save previous limits
	limits_write(&db->config->limits, &tr->log.data);

	// set new limits
	limits_copy(&db->config->limits, limits);

	db_sync_limits(db);
	return true;
}

bool
db_limit_unset(Catalog* self,
               Tr*      tr,
               Str*     name,
               uint64_t mask,
               bool     if_exists)
{
	auto db = catalog_find_db(self, name, false);
	if (! db)
	{
		if (! if_exists)
			error("db '{str}': not exists", name);
		return false;
	}

	// only owner or system
	db_check_ownership(tr, db);

	// update db
	log_ddl(&tr->log, &limit_set_if, NULL, &db->rel);

	// save previous limits
	limits_write(&db->config->limits, &tr->log.data);

	// update limits
	db->config->limits.flags &= ~mask;

	db_sync_limits(db);
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
	auto db = db_of(op->rel);
	apis_free(&db->config->apis);

	uint8_t* pos = log_data_of(self, op);
	apis_read(&db->config->apis, &pos);
}

static LogIf api_set_if =
{
	.commit = api_set_if_commit,
	.abort  = api_set_if_abort
};

bool
db_api_create(Catalog* self,
              Tr*      tr,
              Str*     name,
              Api*     api,
              bool     if_exists)
{
	// PERM_CREATE_API
	check_permission(tr, PERM_CREATE_API);

	auto db = catalog_find_db(self, name, false);
	if (! db)
	{
		if (! if_exists)
			error("db '{str}': not exists", name);
		return false;
	}

	// only owner or system
	db_check_ownership(tr, db);

	// ensure api is not redefined
	auto ref = apis_find(&db->config->apis, &api->uri);
	if (ref)
		error("db '{str}': api '{str}' already exists",
		      name, &api->uri);

	// update db
	log_ddl(&tr->log, &api_set_if, NULL, &db->rel);

	// save previous apis
	apis_write(&db->config->apis, &tr->log.data, 0);

	// add new api
	auto copy = api_copy(api);
	apis_add(&db->config->apis, copy);
	return true;
}

bool
db_api_drop(Catalog* self,
            Tr*      tr,
            Str*     name,
            Str*     uri,
            bool     if_exists)
{
	auto db = catalog_find_db(self, name, false);
	if (! db)
	{
		if (! if_exists)
			error("db '{str}': not exists", name);
		return false;
	}

	// only owner or system
	db_check_ownership(tr, db);

	// ensure api is not redefined
	auto ref = apis_find(&db->config->apis, uri);
	if (! ref)
		error("db '{str}': api '{str}' not exists",
		      name, uri);

	// update db
	log_ddl(&tr->log, &api_set_if, NULL, &db->rel);

	// save previous apis
	apis_write(&db->config->apis, &tr->log.data, 0);

	// remove api
	apis_remove(&db->config->apis, ref);
	api_free(ref);
	return true;
}
