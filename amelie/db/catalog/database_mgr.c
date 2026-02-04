
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
#include <amelie_object.h>
#include <amelie_engine.h>
#include <amelie_catalog.h>

void
database_mgr_init(DatabaseMgr* self)
{
	relation_mgr_init(&self->mgr);
}

void
database_mgr_free(DatabaseMgr* self)
{
	relation_mgr_free(&self->mgr);
}

bool
database_mgr_create(DatabaseMgr*    self,
                    Tr*             tr,
                    DatabaseConfig* config,
                    bool            if_not_exists)
{
	// make sure db does not exists
	auto current = database_mgr_find(self, &config->name, false);
	if (current)
	{
		if (! if_not_exists)
			error("database '%.*s': already exists", str_size(&config->name),
			      str_of(&config->name));
		return false;
	}

	// allocate db and init
	auto db = database_allocate(config);

	// register db
	relation_mgr_create(&self->mgr, tr, &db->rel);
	return true;
}

bool
database_mgr_drop(DatabaseMgr* self,
                  Tr*          tr,
                  Str*         name,
                  bool         if_exists)
{
	auto db = database_mgr_find(self, name, false);
	if (! db)
	{
		if (! if_exists)
			error("database '%.*s': not exists", str_size(name),
			      str_of(name));
		return false;
	}
	if (db->config->system)
		error("database '%.*s': system db cannot be dropped", str_size(name),
		      str_of(name));

	// drop db by object
	relation_mgr_drop(&self->mgr, tr, &db->rel);
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
	auto relation = log_relation_of(self, op);
	auto mgr = database_of(relation->relation);
	// set previous name
	uint8_t* pos = relation->data;
	Str name;
	json_read_string(&pos, &name);
	database_config_set_name(mgr->config, &name);
}

static LogIf rename_if =
{
	.commit = rename_if_commit,
	.abort  = rename_if_abort
};

bool
database_mgr_rename(DatabaseMgr* self,
                    Tr*          tr,
                    Str*         name,
                    Str*         name_new,
                    bool         if_exists)
{
	auto db = database_mgr_find(self, name, false);
	if (! db)
	{
		if (! if_exists)
			error("database '%.*s': not exists", str_size(name),
			      str_of(name));
		return false;
	}

	if (db->config->system)
		error("database '%.*s': system db cannot be renamed", str_size(name),
		       str_of(name));

	// ensure new db does not exists
	if (database_mgr_find(self, name_new, false))
		error("database '%.*s': already exists", str_size(name_new),
		      str_of(name_new));

	// update db
	log_relation(&tr->log, &rename_if, NULL, &db->rel);

	// save name for rollback
	encode_string(&tr->log.data, name);

	// set new name
	database_config_set_name(db->config, name_new);
	return true;
}

void
database_mgr_dump(DatabaseMgr* self, Buf* buf)
{
	// array
	encode_array(buf);
	list_foreach(&self->mgr.list)
	{
		auto db = database_of(list_at(Relation, link));
		if (db->config->system)
			continue;
		database_config_write(db->config, buf, 0);
	}
	encode_array_end(buf);
}

Database*
database_mgr_find(DatabaseMgr* self, Str* name, bool error_if_not_exists)
{
	auto relation = relation_mgr_get(&self->mgr, NULL, name);
	if (! relation)
	{
		if (error_if_not_exists)
			error("database '%.*s': not exists", str_size(name),
			      str_of(name));
		return NULL;
	}
	return database_of(relation);
}

Buf*
database_mgr_list(DatabaseMgr* self, Str* name, int flags)
{
	auto buf = buf_create();
	if (name)
	{
		auto db = database_mgr_find(self, name, false);
		if (db)
			database_config_write(db->config, buf, flags);
		else
			encode_null(buf);
	} else
	{
		encode_array(buf);
		list_foreach(&self->mgr.list)
		{
			auto db = database_of(list_at(Relation, link));
			database_config_write(db->config, buf, flags);
		}
		encode_array_end(buf);
	}
	return buf;
}
