
//
// amelie.
//
// Real-Time SQL Database.
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
udf_mgr_init(UdfMgr* self, UdfIf* iface, void* iface_arg)
{
	self->iface = iface;
	self->iface_arg = iface_arg;
	handle_mgr_init(&self->mgr);
}

void
udf_mgr_free(UdfMgr* self)
{
	handle_mgr_free(&self->mgr);
}

void
udf_mgr_create(UdfMgr*    self,
               Tr*        tr,
               UdfConfig* config,
               bool       if_not_exists)
{
	// make sure udf does not exists
	auto udf = udf_mgr_find(self, &config->schema, &config->name, false);
	if (udf)
	{
		if (! if_not_exists)
			error("function '%.*s': already exists", str_size(&config->name),
			      str_of(&config->name));
		return;
	}

	// allocate udf and init
	udf = udf_allocate(config, self->iface, self->iface_arg);

	// save create operation
	auto op = udf_op_create(config);

	// update mgr
	handle_mgr_create(&self->mgr, tr, LOG_UDF_CREATE, &udf->handle, op);

	// prepare function
	udf_prepare(udf);
}

void
udf_mgr_drop(UdfMgr* self,
             Tr*     tr,
             Str*    schema,
             Str*    name,
             bool    if_exists)
{
	auto udf = udf_mgr_find(self, schema, name, false);
	if (! udf)
	{
		if (! if_exists)
			error("function '%.*s': not exists", str_size(name),
			      str_of(name));
		return;
	}

	// save drop operation
	auto op = udf_op_drop(&udf->config->schema, &udf->config->name);

	// update mgr
	handle_mgr_drop(&self->mgr, tr, LOG_UDF_DROP, &udf->handle, op);
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
	auto udf = udf_of(handle->handle);
	uint8_t* pos = handle->data->start;
	Str schema;
	Str name;
	Str schema_new;
	Str name_new;
	udf_op_rename_read(&pos, &schema, &name, &schema_new, &name_new);
	udf_config_set_schema(udf->config, &schema);
	udf_config_set_name(udf->config, &name);
	buf_free(handle->data);
}

static LogIf rename_if =
{
	.commit = rename_if_commit,
	.abort  = rename_if_abort
};

void
udf_mgr_rename(UdfMgr* self,
               Tr*     tr,
               Str*    schema,
               Str*    name,
               Str*    schema_new,
               Str*    name_new,
               bool    if_exists)
{
	auto udf = udf_mgr_find(self, schema, name, false);
	if (! udf)
	{
		if (! if_exists)
			error("function '%.*s': not exists", str_size(name),
			      str_of(name));
		return;
	}

	// ensure new udf does not exists
	if (udf_mgr_find(self, schema_new, name_new, false))
		error("function '%.*s': already exists", str_size(name_new),
		      str_of(name_new));

	// save rename operation
	auto op = udf_op_rename(schema, name, schema_new, name_new);

	// update mgr
	log_handle(&tr->log, LOG_UDF_RENAME, &rename_if,
	           NULL,
	           &udf->handle, op);

	// set new name
	if (! str_compare(&udf->config->schema, schema_new))
		udf_config_set_schema(udf->config, schema_new);

	if (! str_compare(&udf->config->name, name_new))
		udf_config_set_name(udf->config, name_new);
}

void
udf_mgr_dump(UdfMgr* self, Buf* buf)
{
	// array
	encode_array(buf);
	list_foreach(&self->mgr.list)
	{
		auto udf = udf_of(list_at(Handle, link));
		udf_config_write(udf->config, buf);
	}
	encode_array_end(buf);
}

Buf*
udf_mgr_list(UdfMgr* self)
{
	auto buf = buf_create();
	encode_array(buf);
	list_foreach(&self->mgr.list)
	{
		auto udf = udf_of(list_at(Handle, link));
		udf_config_write(udf->config, buf);
	}
	encode_array_end(buf);
	return buf;
}

Udf*
udf_mgr_find(UdfMgr* self, Str* schema, Str* name,
             bool error_if_not_exists)
{
	auto handle = handle_mgr_get(&self->mgr, schema, name);
	if (! handle)
	{
		if (error_if_not_exists)
			error("function '%.*s': not exists", str_size(name),
			      str_of(name));
		return NULL;
	}
	return udf_of(handle);
}

Udf*
udf_mgr_find_dep(UdfMgr* self, Str* schema, Str* name)
{
	list_foreach(&self->mgr.list)
	{
		auto udf = udf_of(list_at(Handle, link));
		if (udf_depends_on(udf, schema, name))
			return udf;
	}
	return NULL;
}
