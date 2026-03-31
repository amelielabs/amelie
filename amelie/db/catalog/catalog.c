
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
#include <amelie_part.h>
#include <amelie_catalog.h>

void
catalog_init(Catalog*   self,
             CatalogIf* iface,
             void*      iface_arg,
             PartMgrIf* iface_part_mgr,
             void*      iface_part_mgr_arg)
{
	self->iface = iface;
	self->iface_arg = iface_arg;
	user_mgr_init(&self->user_mgr);
	storage_mgr_init(&self->storage_mgr);
	table_mgr_init(&self->table_mgr, &self->storage_mgr,
	               iface_part_mgr,
	               iface_part_mgr_arg);
	branch_mgr_init(&self->branch_mgr, &self->table_mgr);
	udf_mgr_init(&self->udf_mgr, iface->udf_free, iface_arg);
}

void
catalog_free(Catalog* self)
{
	udf_mgr_free(&self->udf_mgr);
	branch_mgr_free(&self->branch_mgr);
	table_mgr_free(&self->table_mgr);
	storage_mgr_free(&self->storage_mgr);
	user_mgr_free(&self->user_mgr);
}

static void
catalog_create(Catalog* self)
{
	Buf buf;
	buf_init(&buf);
	defer_buf(&buf);

	Tr tr;
	tr_init(&tr);
	defer(tr_free, &tr);
	auto on_error = error_catch
	(
		// begin
		tr_begin(&tr);

		Str name;
		str_init(&name);
		str_set_cstr(&name, "main");

		// create main user
		auto user_config = user_config_allocate();
		defer(user_config_free, user_config);
		user_config_set_name(user_config, &name);
		user_config_set_system(user_config, true);

		// set timestamp
		char ts[64];
		auto time = time_us();
		auto size = timestamp_get(time, runtime()->timezone, ts, sizeof(ts));
		Str created_at;
		str_set(&created_at, ts, size);
		user_config_set_created_at(user_config, &created_at);
		user_mgr_create(&self->user_mgr, &tr, user_config, false);

		// create main storage
		auto storage_config = storage_config_allocate();
		defer(storage_config_free, storage_config);
		storage_config_set_name(storage_config, &name);
		Str compression;
		str_set(&compression, "zstd", 4);
		storage_config_set_compression(storage_config, &compression);
		storage_config_set_compression_level(storage_config, 0);
		storage_config_set_system(storage_config, true);
		storage_mgr_create(&self->storage_mgr, &tr, storage_config, false);

		// commit
		tr_commit(&tr);
	);
	if (on_error)
	{
		tr_abort(&tr);
		rethrow();
	}
}

void
catalog_open(Catalog* self, bool bootstrap)
{
	// read catalog file and restore objects
	if (bootstrap)
	{
		// create catalog
		catalog_create(self);
		catalog_write(self);
	} else
	{
		catalog_read(self);
	}
}

void
catalog_close(Catalog* self)
{
	catalog_free(self);
}

Buf*
catalog_status(Catalog* self)
{
	// {}
	auto buf = buf_create();
	encode_obj(buf);

	// users
	encode_raw(buf, "users", 5);
	encode_integer(buf, self->user_mgr.mgr.list_count);

	// storages
	encode_raw(buf, "storages", 8);
	encode_integer(buf, self->storage_mgr.mgr.list_count);

	// tables
	encode_raw(buf, "tables", 6);
	encode_integer(buf, self->table_mgr.mgr.list_count);

	// branches
	encode_raw(buf, "branches", 8);
	encode_integer(buf, self->branch_mgr.mgr.list_count);

	// udfs
	encode_raw(buf, "udfs", 4);
	encode_integer(buf, self->udf_mgr.mgr.list_count);

	encode_obj_end(buf);
	return buf;
}

static void
catalog_validate_udfs(Catalog* self, Str* user, Str* name)
{
	// validate udfs dependencies on the relation
	list_foreach(&self->udf_mgr.mgr.list)
	{
		auto udf = udf_of(list_at(Rel, link));
		if (self->iface->udf_depends(udf, user, name))
			error("function '%.*s' depends on relation '%.*s.%.*s",
			      str_size(udf->rel.name), str_of(udf->rel.name),
			      str_size(user), str_of(user),
			      str_size(name), str_of(name));
	}
}

bool
catalog_execute(Catalog* self, Tr* tr, uint8_t* op, int flags)
{
	// [op, ...]
	auto cmd = ddl_of(op);
	bool write = false;
	switch (cmd) {
	case DDL_GRANT:
	{
		Str     user;
		Str     name;
		Str     to;
		bool    grant;
		int64_t perms;
		grant_op_grant_read(op, &user, &name, &to, &grant, &perms);

		auto if_exists = ddl_if_exists(flags);
		auto rel = catalog_find(self, &user, &name, false);
		if (! rel)
		{
			if (! if_exists)
				error("relation '%.*s': not exists", str_size(&name),
				      str_of(&name));
			break;
		}
		switch (rel->type) {
		case REL_TABLE:
			write = table_mgr_grant(&self->table_mgr, tr, &user, &name, &to,
			                        grant, perms, if_exists);
			break;
		case REL_BRANCH:
			write = branch_mgr_grant(&self->branch_mgr, tr, &user, &name, &to,
			                         grant, perms, if_exists);
			break;
		default:
			abort();
			break;
		}
		break;
	}
	case DDL_USER_CREATE:
	{
		auto config = user_op_create_read(op);
		defer(user_config_free, config);

		auto if_not_exists = ddl_if_not_exists(flags);
		write = user_mgr_create(&self->user_mgr, tr, config, if_not_exists);
		break;
	}
	case DDL_USER_DROP:
	{
		Str  name;
		bool cascade;
		user_op_drop_read(op, &name, &cascade);

		// invalidate auth caches
		auto user = user_mgr_find(&self->user_mgr, &name, false);
		if (user)
			self->iface->user_invalidate(self, user);

		auto if_exists = ddl_if_exists(flags);
		write = cascade_user_drop(self, tr, &name, cascade, if_exists);
		break;
	}
	case DDL_USER_RENAME:
	{
		Str name;
		Str name_new;
		user_op_rename_read(op, &name, &name_new);

		auto if_exists = ddl_if_exists(flags);
		write = cascade_user_rename(self, tr, &name, &name_new, if_exists);
		break;
	}
	case DDL_USER_REVOKE:
	{
		Str name;
		Str revoked_at;
		user_op_revoke_read(op, &name, &revoked_at);

		// invalidate auth caches
		auto user = user_mgr_find(&self->user_mgr, &name, false);
		if (user)
			self->iface->user_invalidate(self, user);

		auto if_exists = ddl_if_exists(flags);
		write = user_mgr_revoke(&self->user_mgr, tr, &name, &revoked_at, if_exists);
		break;
	}
	case DDL_STORAGE_CREATE:
	{
		auto config = storage_op_create_read(op);
		defer(storage_config_free, config);

		auto if_not_exists = ddl_if_not_exists(flags);
		write = storage_mgr_create(&self->storage_mgr, tr, config, if_not_exists);
		break;
	}
	case DDL_STORAGE_DROP:
	{
		Str  name;
		storage_op_drop_read(op, &name);

		auto if_exists = ddl_if_exists(flags);
		write = storage_mgr_drop(&self->storage_mgr, tr, &name, if_exists);
		break;
	}
	case DDL_STORAGE_RENAME:
	{
		Str name;
		Str name_new;
		storage_op_rename_read(op, &name, &name_new);

		auto if_exists = ddl_if_exists(flags);
		write = storage_mgr_rename(&self->storage_mgr, tr, &name, &name_new, if_exists);
		break;
	}
	case DDL_TABLE_CREATE:
	{
		auto config = table_op_create_read(op);
		defer(table_config_free, config);

		auto if_not_exists = ddl_if_not_exists(flags);
		write = table_mgr_create(&self->table_mgr, tr, config, if_not_exists);
		break;
	}
	case DDL_TABLE_DROP:
	{
		Str user;
		Str name;
		table_op_drop_read(op, &user, &name);

		// ensure no other udfs depend on the table
		catalog_validate_udfs(self, &user, &name);

		auto if_exists = ddl_if_exists(flags);
		write = table_mgr_drop(&self->table_mgr, tr, &user, &name, if_exists);
		break;
	}
	case DDL_TABLE_RENAME:
	{
		Str user;
		Str name;
		Str user_new;
		Str name_new;
		table_op_rename_read(op, &user, &name, &user_new, &name_new);

		// ensure user exists
		user_mgr_find(&self->user_mgr, &user_new, true);

		// ensure no other udfs depend on the table
		catalog_validate_udfs(self, &user, &name);

		auto if_exists = ddl_if_exists(flags);
		write = table_mgr_rename(&self->table_mgr, tr, &user, &name,
		                         &user_new, &name_new, if_exists);
		break;
	}
	case DDL_TABLE_TRUNCATE:
	{
		Str user;
		Str name;
		table_op_truncate_read(op, &user, &name);

		auto if_exists = ddl_if_exists(flags);
		write = table_mgr_truncate(&self->table_mgr, tr, &user, &name, if_exists);
		break;
	}
	case DDL_TABLE_SET_IDENTITY:
	{
		Str     user;
		Str     name;
		int64_t value;
		table_op_set_identity_read(op, &user, &name, &value);

		auto if_exists = ddl_if_exists(flags);
		write = table_mgr_set_identity(&self->table_mgr, tr, &user, &name,
		                               value, if_exists);
		break;
	}
	case DDL_TABLE_SET_UNLOGGED:
	{
		Str  user;
		Str  name;
		bool value;
		table_op_set_unlogged_read(op, &user, &name, &value);

		auto if_exists = ddl_if_exists(flags);
		write = table_mgr_set_unlogged(&self->table_mgr, tr, &user, &name,
		                               value, if_exists);
		break;
	}
	case DDL_TABLE_COLUMN_ADD:
	{
		Str  user;
		Str  name;
		auto column = table_op_column_add_read(op, &user, &name);
		defer(column_free, column);

		auto if_exists = ddl_if_exists(flags);
		auto if_column_not_exists = ddl_if_column_not_exists(flags);
		write = table_mgr_column_add(&self->table_mgr, tr, &user, &name,
		                             column,
		                             if_exists,
		                             if_column_not_exists);
		break;
	}
	case DDL_TABLE_COLUMN_DROP:
	{
		Str user;
		Str name;
		Str name_column;
		table_op_column_drop_read(op, &user, &name, &name_column);

		// ensure no other udfs depend on the table
		catalog_validate_udfs(self, &user, &name);

		auto if_exists = ddl_if_exists(flags);
		auto if_column_exists = ddl_if_column_exists(flags);
		write = table_mgr_column_drop(&self->table_mgr, tr, &user, &name,
		                              &name_column,
		                              if_exists,
		                              if_column_exists);
		break;
	}
	case DDL_TABLE_COLUMN_RENAME:
	{
		Str user;
		Str name;
		Str name_column;
		Str name_column_new;
		table_op_column_set_read(op, &user, &name, &name_column, &name_column_new);

		// ensure no other udfs depend on the table
		catalog_validate_udfs(self, &user, &name);

		auto if_exists = ddl_if_exists(flags);
		auto if_column_exists = ddl_if_column_exists(flags);
		write = table_mgr_column_rename(&self->table_mgr, tr, &user, &name, &name_column,
		                                &name_column_new,
		                                if_exists,
		                                if_column_exists);
		break;
	}
	case DDL_TABLE_COLUMN_SET_DEFAULT:
	case DDL_TABLE_COLUMN_SET_STORED:
	case DDL_TABLE_COLUMN_SET_RESOLVED:
	{
		Str user;
		Str name;
		Str name_column;
		Str value;
		table_op_column_set_read(op, &user, &name, &name_column, &value);

		auto if_exists = ddl_if_exists(flags);
		auto if_column_exists = ddl_if_column_exists(flags);
		write = table_mgr_column_set(&self->table_mgr, tr, &user, &name, &name_column,
		                             &value, cmd,
		                             if_exists,
		                             if_column_exists);
		break;
	}
	case DDL_TABLE_STORAGE_ADD:
	{
		Str  user;
		Str  name;
		auto config_pos = table_op_storage_add_read(op, &user, &name);
		auto config = volume_read(&config_pos);
		defer(volume_free, config);

		auto if_exists = ddl_if_exists(flags);
		auto if_not_exists_storage = ddl_if_storage_not_exists(flags);
		auto table = table_mgr_find(&self->table_mgr, &user, &name, !if_exists);
		if (! table)
			break;
		write = table_storage_add(table, tr, config, if_not_exists_storage);
		break;
	}
	case DDL_TABLE_STORAGE_DROP:
	{
		Str user;
		Str name;
		Str name_storage;
		table_op_storage_drop_read(op, &user, &name, &name_storage);

		auto if_exists = ddl_if_exists(flags);
		auto if_exists_storage = ddl_if_storage_exists(flags);
		auto table = table_mgr_find(&self->table_mgr, &user, &name, !if_exists);
		if (! table)
			break;
		write = table_storage_drop(table, tr, &name_storage,
		                           if_exists_storage);
		break;
	}
	case DDL_TABLE_STORAGE_PAUSE:
	{
		Str  user;
		Str  name;
		Str  name_storage;
		bool pause;
		table_op_storage_pause_read(op, &user, &name, &name_storage, &pause);

		auto if_exists = ddl_if_exists(flags);
		auto if_exists_storage = ddl_if_storage_exists(flags);
		auto table = table_mgr_find(&self->table_mgr, &user, &name, !if_exists);
		if (! table)
			break;
		write = table_storage_pause(table, tr, &name_storage,
		                            pause,
		                            if_exists_storage);
		break;
	}
	case DDL_INDEX_CREATE:
	{
		// create index has different processing path and must be
		// done using the user_create_index()
		abort();
		break;
	}
	case DDL_INDEX_DROP:
	{
		Str user;
		Str name;
		Str name_index;
		table_op_index_drop_read(op, &user, &name, &name_index);

		// ensure no other udfs depend on the table
		catalog_validate_udfs(self, &user, &name);

		auto table = table_mgr_find(&self->table_mgr, &user, &name, true);
		auto if_exists = ddl_if_exists(flags);
		write = table_index_drop(table, tr, &name_index, if_exists);
		break;
	}
	case DDL_INDEX_RENAME:
	{
		Str user;
		Str name;
		Str name_index;
		Str name_index_new;
		table_op_index_rename_read(op, &user, &name, &name_index, &name_index_new);

		// ensure no other udfs depend on the table
		catalog_validate_udfs(self, &user, &name);

		auto table = table_mgr_find(&self->table_mgr, &user, &name, true);
		auto if_exists = ddl_if_exists(flags);
		write = table_index_rename(table, tr, &name_index, &name_index_new, if_exists);
		break;
	}
	case DDL_BRANCH_CREATE:
	{
		auto config = branch_op_create_read(op);
		defer(branch_config_free, config);

		auto if_not_exists = ddl_if_not_exists(flags);
		write = branch_mgr_create(&self->branch_mgr, tr, config, if_not_exists);
		break;
	}
	case DDL_BRANCH_DROP:
	{
		Str user;
		Str name;
		branch_op_drop_read(op, &user, &name);

		// ensure no other udfs depend on the branch
		catalog_validate_udfs(self, &user, &name);

		auto if_exists = ddl_if_exists(flags);
		write = branch_mgr_drop(&self->branch_mgr, tr, &user, &name, if_exists);
		break;
	}
	case DDL_BRANCH_RENAME:
	{
		Str user;
		Str name;
		Str user_new;
		Str name_new;
		branch_op_rename_read(op, &user, &name, &user_new, &name_new);

		// ensure no other udfs depend on the branch
		catalog_validate_udfs(self, &user, &name);

		auto if_exists = ddl_if_exists(flags);
		write = branch_mgr_rename(&self->branch_mgr, tr, &user, &name, &user_new, &name_new, if_exists);
		break;
	}
	case DDL_UDF_CREATE:
	{
		auto config = udf_op_create_read(op);
		defer(udf_config_free, config);

		// create or replace
		write = udf_mgr_create(&self->udf_mgr, tr, config, false);

		// compile on creation
		if (write)
		{
			auto udf = udf_mgr_find(&self->udf_mgr, &config->user, &config->name, true);
			self->iface->udf_compile(self, udf);
		}
		break;
	}
	case DDL_UDF_REPLACE:
	{
		auto config = udf_op_replace_read(op);
		defer(udf_config_free, config);

		// allow to replace udf only if it has identical arguments
		// and return type

		// create or replace
		auto udf = udf_mgr_find(&self->udf_mgr, &config->user, &config->name, false);
		if (udf)
		{
			// validate types
			udf_mgr_replace_validate(&self->udf_mgr, config, udf);

			// create and compile new udf
			auto replace = udf_allocate(config, self->udf_mgr.free, self->udf_mgr.free_arg);
			defer(rel_free, replace);
			self->iface->udf_compile(self, replace);

			udf_mgr_replace(&self->udf_mgr, tr, udf, replace);
			write = true;
			break;
		}

		// create
		write = udf_mgr_create(&self->udf_mgr, tr, config, false);

		// compile on creation
		if (write)
		{
			udf = udf_mgr_find(&self->udf_mgr, &config->user, &config->name, true);
			self->iface->udf_compile(self, udf);
		}
		break;
	}
	case DDL_UDF_DROP:
	{
		Str user;
		Str name;
		udf_op_drop_read(op, &user, &name);

		// ensure no other udfs depend on it
		catalog_validate_udfs(self, &user, &name);

		auto if_exists = ddl_if_exists(flags);
		write = udf_mgr_drop(&self->udf_mgr, tr, &user, &name, if_exists);
		break;
	}
	case DDL_UDF_RENAME:
	{
		Str user;
		Str name;
		Str user_new;
		Str name_new;
		udf_op_rename_read(op, &user, &name, &user_new, &name_new);

		// ensure user exists and not system
		user_mgr_find(&self->user_mgr, &user_new, true);

		// ensure no other udfs depend on it
		catalog_validate_udfs(self, &user, &name);

		auto if_exists = ddl_if_exists(flags);
		write = udf_mgr_rename(&self->udf_mgr, tr, &user, &name,
		                       &user_new, &name_new, if_exists);
		break;
	}
	default:
		error("unrecognized ddl operation id: %d", (int)cmd);
		break;
	}

	if (write)
	{
		log_persist_rel(&tr->log, op);
		return true;
	}
	return false;
}

hot Rel*
catalog_find(Catalog* self, Str* user, Str* name, bool error_if_not_exists)
{
	auto table = table_mgr_find(&self->table_mgr, user, name, false);
	if (table)
		return &table->rel;

	auto branch = branch_mgr_find(&self->branch_mgr, user, name, false);
	if (branch)
		return &branch->rel;

	auto udf = udf_mgr_find(&self->udf_mgr, user, name, false);
	if (udf)
		return &udf->rel;

	if (error_if_not_exists)
		error("relation '%.*s': not exists", str_size(name),
		       str_of(name));
	return NULL;
}

hot Table*
catalog_find_table(Catalog* self, Str* user, Str* name,
                   bool     error_if_not_exists)
{
	return table_mgr_find(&self->table_mgr, user, name, error_if_not_exists);
}
