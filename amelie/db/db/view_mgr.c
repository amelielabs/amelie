
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

#include <amelie_runtime.h>
#include <amelie_io.h>
#include <amelie_lib.h>
#include <amelie_data.h>
#include <amelie_config.h>
#include <amelie_row.h>
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_partition.h>
#include <amelie_checkpoint.h>
#include <amelie_wal.h>
#include <amelie_db.h>

void
view_mgr_init(ViewMgr* self)
{
	handle_mgr_init(&self->mgr);
}

void
view_mgr_free(ViewMgr* self)
{
	handle_mgr_free(&self->mgr);
}

void
view_mgr_create(ViewMgr*    self,
                Tr*         tr,
                ViewConfig* config,
                bool        if_not_exists)
{
	// make sure view does not exists
	auto current = view_mgr_find(self, &config->schema, &config->name, false);
	if (current)
	{
		if (! if_not_exists)
			error("view '%.*s': already exists", str_size(&config->name),
			      str_of(&config->name));
		return;
	}

	// allocate view and init
	auto view = view_allocate(config);

	// save create view operation
	auto op = view_op_create(config);

	// update views
	handle_mgr_create(&self->mgr, tr, LOG_VIEW_CREATE, &view->handle, op);
}

void
view_mgr_drop(ViewMgr* self,
              Tr*      tr,
              Str*     schema,
              Str*     name,
              bool     if_exists)
{
	auto view = view_mgr_find(self, schema, name, false);
	if (! view)
	{
		if (! if_exists)
			error("view '%.*s': not exists", str_size(name),
			      str_of(name));
		return;
	}

	// save drop view operation
	auto op = view_op_drop(&view->config->schema, &view->config->name);

	// update mgr
	handle_mgr_drop(&self->mgr, tr, LOG_VIEW_DROP, &view->handle, op);
}

static void
rename_if_commit(Log* self, LogOp* op)
{
	buf_free(log_handle_of(self, op)->data);
}

static void
rename_if_abort(Log* self, LogOp* op)
{
	auto handle = log_handle_of(self, op);
	auto view = view_of(handle->handle);
	uint8_t* pos = handle->data->start;
	Str schema;
	Str name;
	Str schema_new;
	Str name_new;
	view_op_rename_read(&pos, &schema, &name, &schema_new, &name_new);
	view_config_set_schema(view->config, &schema);
	view_config_set_name(view->config, &name);
	buf_free(handle->data);
}

static LogIf rename_if =
{
	.commit = rename_if_commit,
	.abort  = rename_if_abort
};

void
view_mgr_rename(ViewMgr* self,
                Tr*      tr,
                Str*     schema,
                Str*     name,
                Str*     schema_new,
                Str*     name_new,
                bool     if_exists)
{
	auto view = view_mgr_find(self, schema, name, false);
	if (! view)
	{
		if (! if_exists)
			error("view '%.*s': not exists", str_size(name),
			      str_of(name));
		return;
	}

	// ensure new view does not exists
	if (view_mgr_find(self, schema_new, name_new, false))
		error("view '%.*s': already exists", str_size(name_new),
		      str_of(name_new));

	// save rename view operation
	auto op = view_op_rename(schema, name, schema_new, name_new);

	// update view
	log_handle(&tr->log, LOG_VIEW_RENAME, &rename_if,
	           NULL,
	           &view->handle, NULL, op);

	// set new view name
	if (! str_compare(&view->config->schema, schema_new))
		view_config_set_schema(view->config, schema_new);

	if (! str_compare(&view->config->name, name_new))
		view_config_set_name(view->config, name_new);
}

void
view_mgr_dump(ViewMgr* self, Buf* buf)
{
	// array
	encode_array(buf);
	list_foreach(&self->mgr.list)
	{
		auto view = view_of(list_at(Handle, link));
		view_config_write(view->config, buf);
	}
	encode_array_end(buf);
}

View*
view_mgr_find(ViewMgr* self, Str* schema, Str* name,
              bool error_if_not_exists)
{
	auto handle = handle_mgr_get(&self->mgr, schema, name);
	if (! handle)
	{
		if (error_if_not_exists)
			error("view '%.*s': not exists", str_size(name),
			      str_of(name));
		return NULL;
	}
	return view_of(handle);
}

Buf*
view_mgr_list(ViewMgr* self)
{
	auto buf = buf_create();
	encode_obj(buf);
	list_foreach(&self->mgr.list)
	{
		auto view = view_of(list_at(Handle, link));
		encode_target(buf, &view->config->schema, &view->config->name);
		view_config_write(view->config, buf);
	}
	encode_obj_end(buf);
	return buf;
}
