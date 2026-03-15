
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

void
synonym_mgr_init(SynonymMgr* self)
{
	rel_mgr_init(&self->mgr);
}

void
synonym_mgr_free(SynonymMgr* self)
{
	rel_mgr_free(&self->mgr);
}

bool
synonym_mgr_create(SynonymMgr*    self,
                   Tr*            tr,
                   SynonymConfig* config,
                   bool           if_not_exists)
{
	// make sure synonym does not exists
	auto current = synonym_mgr_find(self, &config->db, &config->name, false);
	if (current)
	{
		if (! if_not_exists)
			error("synonym '%.*s': already exists", str_size(&config->name),
			      str_of(&config->name));
		return false;
	}

	// create synonym
	auto synonym = synonym_allocate(config);
	rel_mgr_create(&self->mgr, tr, &synonym->rel);
	return true;
}

void
synonym_mgr_drop_of(SynonymMgr* self, Tr* tr, Synonym* synonym)
{
	// drop synonym by object
	rel_mgr_drop(&self->mgr, tr, &synonym->rel);
}

bool
synonym_mgr_drop(SynonymMgr* self, Tr* tr, Str* db, Str* name,
                 bool        if_exists)
{
	auto synonym = synonym_mgr_find(self, db, name, false);
	if (! synonym)
	{
		if (! if_exists)
			error("synonym '%.*s': not exists", str_size(name),
			      str_of(name));
		return false;
	}
	synonym_mgr_drop_of(self, tr, synonym);
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
	auto rel = log_rel_of(self, op);
	auto synonym = synonym_of(rel->rel);
	uint8_t* pos = rel->data;
	Str db;
	Str name;
	json_read_string(&pos, &db);
	json_read_string(&pos, &name);
	synonym_config_set_db(synonym->config, &db);
	synonym_config_set_name(synonym->config, &name);
}

static LogIf rename_if =
{
	.commit = rename_if_commit,
	.abort  = rename_if_abort
};

bool
synonym_mgr_rename(SynonymMgr* self,
                   Tr*         tr,
                   Str*        db,
                   Str*        name,
                   Str*        db_new,
                   Str*        name_new,
                   bool        if_exists)
{
	auto synonym = synonym_mgr_find(self, db, name, false);
	if (! synonym)
	{
		if (! if_exists)
			error("synonym '%.*s': not exists", str_size(name),
			      str_of(name));
		return false;
	}

	// ensure new synonym does not exists
	if (synonym_mgr_find(self, db_new, name_new, false))
		error("synonym '%.*s': already exists", str_size(name_new),
		      str_of(name_new));

	// update synonym
	log_rel(&tr->log, &rename_if, NULL, &synonym->rel);

	// save previous name
	encode_string(&tr->log.data, &synonym->config->db);
	encode_string(&tr->log.data, &synonym->config->name);

	// set new name
	if (! str_compare_case(&synonym->config->db, db_new))
		synonym_config_set_db(synonym->config, db_new);

	if (! str_compare_case(&synonym->config->name, name_new))
		synonym_config_set_name(synonym->config, name_new);

	return true;
}

void
synonym_mgr_dump(SynonymMgr* self, Buf* buf)
{
	// array
	encode_array(buf);
	list_foreach(&self->mgr.list)
	{
		auto synonym = synonym_of(list_at(Rel, link));
		synonym_config_write(synonym->config, buf, 0);
	}
	encode_array_end(buf);
}

Synonym*
synonym_mgr_find(SynonymMgr* self, Str* db, Str* name,
             bool    error_if_not_exists)
{
	auto rel = rel_mgr_get(&self->mgr, db, name);
	if (! rel)
	{
		if (error_if_not_exists)
			error("synonym '%.*s': not exists", str_size(name),
			      str_of(name));
		return NULL;
	}
	return synonym_of(rel);
}

Buf*
synonym_mgr_list(SynonymMgr* self, Str* db, Str* name, int flags)
{
	auto buf = buf_create();
	if (db && name)
	{
		// show synonym
		auto synonym = synonym_mgr_find(self, db, name, false);
		if (synonym)
			synonym_config_write(synonym->config, buf, flags);
		else
			encode_null(buf);
		return buf;
	}

	// show synonyms
	encode_array(buf);
	list_foreach(&self->mgr.list)
	{
		auto synonym = synonym_of(list_at(Rel, link));
		if (db && !str_compare_case(&synonym->config->db, db))
			continue;
		synonym_config_write(synonym->config, buf, flags);
	}
	encode_array_end(buf);
	return buf;
}
