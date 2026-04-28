
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
catalog_init(Catalog*   self,
             CatalogIf* iface,
             void*      iface_arg,
             PartMgrIf* iface_part,
             void*      iface_part_arg,
             Cdc*       cdc)
{
	self->iface          = iface;
	self->iface_arg      = iface_arg;
	self->iface_part     = iface_part;
	self->iface_part_arg = iface_part_arg;
	self->cdc            = cdc;

	rel_mgr_init(&self->users);
	rel_mgr_init(&self->rels);
	storage_mgr_init(&self->storage_mgr);

	// prepare subscription columns
	auto columns = &self->cdc_columns;
	columns_init(columns);

	// lsn
	auto column = column_allocate();
	Str name;
	str_set(&name, "lsn", 3);
	column_set_name(column, &name);
	column_set_type(column, TYPE_INT, sizeof(int64_t));
	columns_add(columns, column);

	// lsn_op
	column = column_allocate();
	str_set(&name, "lsn_op", 6);
	column_set_name(column, &name);
	column_set_type(column, TYPE_INT, sizeof(int32_t));
	columns_add(columns, column);

	// cmd
	column = column_allocate();
	str_set(&name, "cmd", 3);
	column_set_name(column, &name);
	column_set_type(column, TYPE_STRING, 0);
	columns_add(columns, column);

	// row
	column = column_allocate();
	str_set(&name, "row", 3);
	column_set_name(column, &name);
	column_set_type(column, TYPE_JSON, 0);
	columns_add(columns, column);
}

void
catalog_free(Catalog* self)
{
	rel_mgr_free(&self->rels);
	storage_mgr_free(&self->storage_mgr);
	rel_mgr_free(&self->users);
	columns_free(&self->cdc_columns);
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
		Str name;
		str_init(&name);
		str_set_cstr(&name, "main");

		// create main user
		auto user_config = user_config_allocate();
		defer(user_config_free, user_config);
		user_config_set_name(user_config, &name);
		user_config_set_superuser(user_config, true);

		// set timestamp
		char ts[64];
		auto time = time_us();
		auto size = timestamp_get(time, runtime()->timezone, ts, sizeof(ts));
		Str created_at;
		str_set(&created_at, ts, size);
		user_config_set_created_at(user_config, &created_at);
		user_mgr_create(self, &tr, user_config, false);

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

void
catalog_status(Catalog* self, Buf* buf)
{
	// {}
	encode_obj(buf);

	// users
	encode_raw(buf, "users", 5);
	encode_int(buf, self->users.list_count);

	// storages
	encode_raw(buf, "storages", 8);
	encode_int(buf, self->storage_mgr.mgr.list_count);

	// rels
	encode_raw(buf, "rels", 4);
	encode_int(buf, self->rels.list_count);

	encode_obj_end(buf);
}

static void
catalog_validate_udfs(Catalog* self, Str* user, Str* name)
{
	// validate udfs dependencies on the relation
	list_foreach(&self->rels.list)
	{
		auto rel = list_at(Rel, link);
		if (rel->type != REL_UDF)
			continue;
		auto udf = udf_of(rel);
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
		// PERM_GRANT
		check_user(tr, PERM_GRANT);

		Str     user;
		Str     name;
		Str     to;
		bool    grant;
		int64_t perms;
		grant_op_grant_read(op, &user, &name, &to, &grant, &perms);

		// user grants
		if (str_empty(&user))
		{
			// PERM_SYSTEM for system wide grants
			if (((perms & PERM_SYSTEM) > 0) ||
			    ((perms & PERM_CREATE_USER) > 0))
				check_user(tr, PERM_SYSTEM);
			write = user_mgr_grant(self, tr, &to, grant, perms, 0);
			break;
		}

		// relations
		auto rel = catalog_find(self, REL_UNDEF, &user, &name, true);
		switch (rel->type) {
		case REL_TABLE:
			write = table_mgr_grant(self, tr, &user, &name, &to, grant, perms, 0);
			break;
		case REL_BRANCH:
			write = branch_mgr_grant(self, tr, &user, &name, &to, grant, perms, 0);
			break;
		case REL_UDF:
			write = udf_mgr_grant(self, tr, &user, &name, &to, grant, perms, 0);
			break;
		case REL_TOPIC:
			write = topic_mgr_grant(self, tr, &user, &name, &to, grant, perms, 0);
			break;
		case REL_SUBSCRIPTION:
			write = sub_mgr_grant(self, tr, &user, &name, &to, grant, perms, 0);
			break;
		default:
			abort();
			break;
		}
		break;
	}
	case DDL_USER_CREATE:
	{
		// PERM_CREATE_USER
		check_user(tr, PERM_CREATE_USER);

		auto config = user_op_create_read(op);
		defer(user_config_free, config);
		auto if_not_exists = ddl_if_not_exists(flags);
		write = user_mgr_create(self, tr, config, if_not_exists);
		break;
	}
	case DDL_USER_DROP:
	{
		Str  name;
		bool cascade;
		user_op_drop_read(op, &name, &cascade);

		// invalidate auth caches
		auto user = catalog_find_user(self, &name, false);
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
		auto user = catalog_find_user(self, &name, false);
		if (user)
			self->iface->user_invalidate(self, user);

		auto if_exists = ddl_if_exists(flags);
		write = user_mgr_revoke(self, tr, &name, &revoked_at, if_exists);
		break;
	}
	case DDL_STORAGE_CREATE:
	{
		// PERM_SYSTEM
		check_user(tr, PERM_SYSTEM);

		auto config = storage_op_create_read(op);
		defer(storage_config_free, config);
		auto if_not_exists = ddl_if_not_exists(flags);
		write = storage_mgr_create(&self->storage_mgr, tr, config, if_not_exists);
		break;
	}
	case DDL_STORAGE_DROP:
	{
		// PERM_SYSTEM
		check_user(tr, PERM_SYSTEM);

		Str  name;
		storage_op_drop_read(op, &name);
		auto if_exists = ddl_if_exists(flags);
		write = storage_mgr_drop(&self->storage_mgr, tr, &name, if_exists);
		break;
	}
	case DDL_STORAGE_RENAME:
	{
		// PERM_SYSTEM
		check_user(tr, PERM_SYSTEM);

		Str name;
		Str name_new;
		storage_op_rename_read(op, &name, &name_new);
		auto if_exists = ddl_if_exists(flags);
		write = storage_mgr_rename(&self->storage_mgr, tr, &name, &name_new, if_exists);
		break;
	}
	case DDL_TABLE_CREATE:
	{
		// PERM_CREATE_TABLE
		check_user(tr, PERM_CREATE_TABLE);

		auto config = table_op_create_read(op);
		defer(table_config_free, config);
		auto if_not_exists = ddl_if_not_exists(flags);
		write = table_mgr_create(self, tr, config, if_not_exists);
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
		write = table_mgr_drop(self, tr, &user, &name, if_exists);
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
		catalog_find_user(self, &user_new, true);

		// ensure no other udfs depend on the table
		catalog_validate_udfs(self, &user, &name);

		auto if_exists = ddl_if_exists(flags);
		write = table_mgr_rename(self, tr, &user, &name, &user_new, &name_new, if_exists);
		break;
	}
	case DDL_TABLE_TRUNCATE:
	{
		Str user;
		Str name;
		table_op_truncate_read(op, &user, &name);

		auto if_exists = ddl_if_exists(flags);
		write = table_mgr_truncate(self, tr, &user, &name, if_exists);
		break;
	}
	case DDL_TABLE_SET_IDENTITY:
	{
		Str     user;
		Str     name;
		int64_t value;
		table_op_set_identity_read(op, &user, &name, &value);

		auto if_exists = ddl_if_exists(flags);
		write = table_mgr_set_identity(self, tr, &user, &name, value, if_exists);
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
		write = table_mgr_column_add(self, tr, &user, &name,
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
		write = table_mgr_column_drop(self, tr, &user, &name,
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
		write = table_mgr_column_rename(self, tr, &user, &name, &name_column,
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
		write = table_mgr_column_set(self, tr, &user, &name, &name_column,
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
		auto table = catalog_find_table(self, &user, &name, !if_exists);
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
		auto table = catalog_find_table(self, &user, &name, !if_exists);
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
		auto table = catalog_find_table(self, &user, &name, !if_exists);
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

		auto table = catalog_find_table(self, &user, &name, true);
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

		auto table = catalog_find_table(self, &user, &name, true);
		auto if_exists = ddl_if_exists(flags);
		write = table_index_rename(table, tr, &name_index, &name_index_new, if_exists);
		break;
	}
	case DDL_BRANCH_CREATE:
	{
		// PERM_CREATE_BRANCH
		check_user(tr, PERM_CREATE_BRANCH);

		auto config = branch_op_create_read(op);
		defer(branch_config_free, config);
		auto if_not_exists = ddl_if_not_exists(flags);
		write = branch_mgr_create(self, tr, config, if_not_exists);
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
		write = branch_mgr_drop(self, tr, &user, &name, if_exists);
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
		write = branch_mgr_rename(self, tr, &user, &name, &user_new, &name_new, if_exists);
		break;
	}
	case DDL_UDF_CREATE:
	{
		// PERM_CREATE_FUNCTION
		check_user(tr, PERM_CREATE_FUNCTION);

		auto config = udf_op_create_read(op);
		defer(udf_config_free, config);

		// create or replace
		write = udf_mgr_create(self, tr, config, false);

		// compile on creation
		if (write)
		{
			auto udf = catalog_find_udf(self, &config->user, &config->name, true);
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
		auto udf = catalog_find_udf(self, &config->user, &config->name, false);
		if (udf)
		{
			// validate types
			udf_mgr_replace_validate(self, tr, config, udf);

			// create and compile new udf
			auto replace = udf_allocate(config, self->iface->udf_free, self->iface_arg);
			defer(rel_free, replace);
			self->iface->udf_compile(self, replace);

			udf_mgr_replace(self, tr, udf, replace);
			write = true;
			break;
		}

		// create
		write = udf_mgr_create(self, tr, config, false);

		// compile on creation
		if (write)
		{
			udf = catalog_find_udf(self, &config->user, &config->name, true);
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
		write = udf_mgr_drop(self, tr, &user, &name, if_exists);
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
		catalog_find_user(self, &user_new, true);

		// ensure no other udfs depend on it
		catalog_validate_udfs(self, &user, &name);

		auto if_exists = ddl_if_exists(flags);
		write = udf_mgr_rename(self, tr, &user, &name, &user_new, &name_new, if_exists);
		break;
	}
	case DDL_TOPIC_CREATE:
	{
		// PERM_CREATE_TOPIC
		check_user(tr, PERM_CREATE_TOPIC);

		auto config = topic_op_create_read(op);
		defer(topic_config_free, config);
		auto if_not_exists = ddl_if_not_exists(flags);
		write = topic_mgr_create(self, tr, config, if_not_exists);
		break;
	}
	case DDL_TOPIC_DROP:
	{
		Str user;
		Str name;
		topic_op_drop_read(op, &user, &name);

		// ensure no other udfs depend on the topic
		catalog_validate_udfs(self, &user, &name);

		auto if_exists = ddl_if_exists(flags);
		write = topic_mgr_drop(self, tr, &user, &name, if_exists);
		break;
	}
	case DDL_TOPIC_RENAME:
	{
		Str user;
		Str name;
		Str user_new;
		Str name_new;
		topic_op_rename_read(op, &user, &name, &user_new, &name_new);

		// ensure no other udfs depend on the topic catalog_validate_udfs(self, &user, &name);

		auto if_exists = ddl_if_exists(flags);
		write = topic_mgr_rename(self, tr, &user, &name, &user_new, &name_new, if_exists);
		break;
	}
	case DDL_SUB_CREATE:
	{
		// PERM_CREATE_SUB
		check_user(tr, PERM_CREATE_SUB);

		auto config = sub_op_create_read(op);
		defer(sub_config_free, config);
		auto if_not_exists = ddl_if_not_exists(flags);
		write = sub_mgr_create(self, tr, config, if_not_exists);
		break;
	}
	case DDL_SUB_DROP:
	{
		Str user;
		Str name;
		sub_op_drop_read(op, &user, &name);

		// ensure no other udfs depend on the sub
		catalog_validate_udfs(self, &user, &name);

		auto if_exists = ddl_if_exists(flags);
		write = sub_mgr_drop(self, tr, &user, &name, if_exists);
		break;
	}
	case DDL_SUB_RENAME:
	{
		Str user;
		Str name;
		Str user_new;
		Str name_new;
		sub_op_rename_read(op, &user, &name, &user_new, &name_new);

		// ensure no other udfs depend on the sub
		catalog_validate_udfs(self, &user, &name);

		auto if_exists = ddl_if_exists(flags);
		write = sub_mgr_rename(self, tr, &user, &name, &user_new, &name_new, if_exists);
		break;
	}
	default:
		error("unrecognized ddl operation id: %d", (int)cmd);
		break;
	}

	if (write)
	{
		log_persist_cmd(&tr->log, NULL, op);
		return true;
	}
	return false;
}
