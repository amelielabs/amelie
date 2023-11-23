
//
// monotone
//	
// SQL OLTP database
//

#include <monotone_runtime.h>
#include <monotone_io.h>
#include <monotone_data.h>
#include <monotone_lib.h>
#include <monotone_config.h>
#include <monotone_def.h>
#include <monotone_transaction.h>
#include <monotone_storage.h>
#include <monotone_snapshot.h>
#include <monotone_wal.h>
#include <monotone_db.h>

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
	guard(guard, view_free, view);

	// save create view operation
	auto op = view_op_create(config);

	// update views
	handle_mgr_write(&self->mgr, trx, LOG_VIEW_CREATE, &view->handle, op);

	buf_unpin(op);
	unguard(&guard);
}

static void
view_mgr_drop_view(ViewMgr* self, Transaction* trx, View* view)
{
	// save drop view operation
	auto op = view_op_drop(&view->config->schema, &view->config->name);

	// drop by name
	Handle drop;
	handle_init(&drop);
	handle_set_schema(&drop, &view->config->schema);
	handle_set_name(&drop, &view->config->name);

	// update mgr
	handle_mgr_write(&self->mgr, trx, LOG_VIEW_DROP, &drop, op);

	buf_unpin(op);
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

	// drop view
	view_mgr_drop_view(self, trx, view);
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

	// allocate new view
	auto update = view_allocate(view->config);
	guard(guard, view_free, update);

	// set new view name
	view_config_set_schema(update->config, schema_new);
	view_config_set_name(update->config, name_new);

	// drop previous view object
	view_mgr_drop_view(self, trx, view);

	// save rename view operation
	auto op = view_op_rename(schema, name, schema_new, name_new);

	// update views
	handle_mgr_write(&self->mgr, trx, LOG_VIEW_RENAME, &update->handle, op);

	buf_unpin(op);
	unguard(&guard);
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
	auto buf = msg_create(MSG_OBJECT);
	// array
	encode_array(buf, self->mgr.list_count);
	list_foreach(&self->mgr.list)
	{
		// {}
		auto view = view_of(list_at(Handle, link));
		view_config_write(view->config, buf);
	}
	msg_end(buf);
	return buf;
}
