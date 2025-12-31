
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
#include <amelie_volume>
#include <amelie_catalog.h>

static void
cascade_db_drop_execute(Catalog* self, Tr* tr, Str* db, bool drop)
{
	// validate or drop all objects matching the database

	// tables
	list_foreach_safe(&self->table_mgr.mgr.list)
	{
		auto table = table_of(list_at(Relation, link));
		if (! str_compare_case(&table->config->db, db))
			continue;
		if (drop)
			table_mgr_drop_of(&self->table_mgr, tr, table);
		else
			error("table '%.*s' depends on db '%.*s",
			      str_size(&table->config->name), str_of(&table->config->name),
			      str_size(db), str_of(db));
	}

	// udfs
	list_foreach_safe(&self->udf_mgr.mgr.list)
	{
		auto udf = udf_of(list_at(Relation, link));
		if (! str_compare_case(&udf->config->db, db))
			continue;
		if (drop)
			udf_mgr_drop_of(&self->udf_mgr, tr, udf);
		else
			error("function '%.*s' depends on db '%.*s",
			      str_size(&udf->config->name), str_of(&udf->config->name),
			      str_size(db), str_of(db));
	}
}

bool
cascade_db_drop(Catalog* self, Tr* tr, Str* name,
                bool     cascade,
                bool     if_exists)
{
	auto db = db_mgr_find(&self->db_mgr, name, false);
	if (! db)
	{
		if (! if_exists)
			error("db '%.*s': not exists", str_size(name),
			      str_of(name));
		return false;
	}

	if (db->config->system)
		error("db '%.*s': system db cannot be dropped", str_size(name),
		      str_of(name));

	// validate or drop all objects matching the database
	cascade_db_drop_execute(self, tr, name, cascade);

	db_mgr_drop(&self->db_mgr, tr, name, false);
	return true;
}

static void
cascade_db_rename_execute(Catalog* self, Tr* tr, Str* db, Str* db_new)
{
	// tables
	list_foreach_safe(&self->table_mgr.mgr.list)
	{
		auto table = table_of(list_at(Relation, link));
		if (str_compare_case(&table->config->db, db))
			table_mgr_rename(&self->table_mgr, tr, &table->config->db,
			                 &table->config->name,
			                 db_new,
			                 &table->config->name, false);
	}

	// udfs
	list_foreach_safe(&self->udf_mgr.mgr.list)
	{
		auto udf = udf_of(list_at(Relation, link));
		if (str_compare_case(&udf->config->db, db))
			error("function '%.*s' is using database '%.*s",
			      str_size(udf->rel.name), str_of(udf->rel.name),
			      str_size(db), str_of(db));
	}
}

bool
cascade_db_rename(Catalog* self, Tr* tr,
                  Str*     name,
                  Str*     name_new,
                  bool     if_exists)
{
	auto db = db_mgr_find(&self->db_mgr, name, false);
	if (! db)
	{
		if (! if_exists)
			error("db '%.*s': not exists", str_size(name),
			      str_of(name));
		return false;
	}

	if (db->config->system)
		error("db '%.*s': system db cannot be renamed", str_size(name),
		      str_of(name));

	// rename all database objects
	cascade_db_rename_execute(self, tr, name, name_new);

	// rename db
	db_mgr_rename(&self->db_mgr, tr, name, name_new, false);
	return true;
}
