
//
// sonata.
//
// Real-Time SQL Database.
//

#include <sonata_runtime.h>
#include <sonata_io.h>
#include <sonata_lib.h>
#include <sonata_data.h>
#include <sonata_config.h>
#include <sonata_def.h>
#include <sonata_transaction.h>
#include <sonata_index.h>
#include <sonata_partition.h>
#include <sonata_wal.h>
#include <sonata_db.h>

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
view_mgr_create(ViewMgr*     self,
                Transaction* trx,
                ViewConfig*  config,
                bool         if_not_exists)
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
	guard(view_free, view);

	// save create view operation
	auto op = view_op_create(config);
	guard_buf(op);

	// update views
	handle_mgr_create(&self->mgr, trx, LOG_VIEW_CREATE, &view->handle, op);

	unguard();
	unguard();
}

void
view_mgr_drop(ViewMgr*     self,
              Transaction* trx,
              Str*         schema,
              Str*         name,
              bool         if_exists)
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
	guard_buf(op);

	// update mgr
	handle_mgr_drop(&self->mgr, trx, LOG_VIEW_DROP, &view->handle, op);
	unguard();
}

static void
view_mgr_rename_abort(LogOp* op)
{
	auto self = view_of(op->handle.handle);
	uint8_t* pos = op->handle.data->start;
	Str schema;
	Str name;
	Str schema_new;
	Str name_new;
	view_op_rename_read(&pos, &schema, &name, &schema_new, &name_new);
	view_config_set_schema(self->config, &schema);
	view_config_set_name(self->config, &name);
	buf_free(op->handle.data);
}

void
view_mgr_rename(ViewMgr*     self,
                Transaction* trx,
                Str*         schema,
                Str*         name,
                Str*         schema_new,
                Str*         name_new,
                bool         if_exists)
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
	guard_buf(op);

	// update view
	handle_mgr_alter(&self->mgr, trx, LOG_VIEW_RENAME, &view->handle, op,
	                 view_mgr_rename_abort, NULL);
	unguard();

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
	encode_array(buf, self->mgr.list_count);
	list_foreach(&self->mgr.list)
	{
		auto view = view_of(list_at(Handle, link));
		view_config_write(view->config, buf);
	}
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
	auto buf = buf_begin();
	encode_array(buf, self->mgr.list_count);
	list_foreach(&self->mgr.list)
	{
		auto view = view_of(list_at(Handle, link));
		view_config_write(view->config, buf);
	}
	return buf_end(buf);
}
