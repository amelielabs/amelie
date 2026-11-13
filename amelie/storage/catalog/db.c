
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

	// free relations
	catalog_free(&self->catalog);

	// free pages
	page_mgr_free(&self->page_mgr);
	checkpoints_free(&self->checkpoints);

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
	checkpoints_init(&self->checkpoints, &self->config->id);

	// prepare arena
	page_mgr_init(&self->page_mgr);
	usage_init(&self->memory, "memory");

	db_sync_limits(self);

	// prepare catalog
	catalog_init(&self->catalog,
	             databases->iface,
	             databases->iface_arg,
	             &self->memory,
	             &self->rel);

	// set relation
	auto rel = &self->rel;
	rel_init(rel, REL_DB);
	rel_set_id(rel, &self->config->id);
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

	// make sure db id is unique
	auto rel_id = catalog_find_by(self, REL_UNDEF, &config->id, false);
	if (rel_id)
		error("db '{str}': conflict by uuid with existing database",
		       rel_id->name);

	// create db
	db = db_allocate(self, config);
	rels_create(&self->rels, tr, &db->rel);

	// update timestamps
	db_sync_revoke(db);

	// deploy db
	self->iface->attach(self, db);
	return true;
}

bool
db_drop(Catalog* self,
        Tr*      tr,
        Str*     name,
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
		error("db '{str}': system db cannot be dropped", name);

	// invalidate tokens
	db_invalidate(db);

	// drop db by object
	rels_drop(&self->rels, tr, &db->rel);
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
