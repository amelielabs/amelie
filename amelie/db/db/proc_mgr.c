
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
#include <amelie_json.h>
#include <amelie_config.h>
#include <amelie_row.h>
#include <amelie_heap.h>
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_partition.h>
#include <amelie_checkpoint.h>
#include <amelie_wal.h>
#include <amelie_db.h>

void
proc_mgr_init(ProcMgr* self, ProcIf* iface, void* iface_arg)
{
	self->iface = iface;
	self->iface_arg = iface_arg;
	relation_mgr_init(&self->mgr);
}

void
proc_mgr_free(ProcMgr* self)
{
	relation_mgr_free(&self->mgr);
}

void
proc_mgr_create(ProcMgr*    self,
                Tr*         tr,
                ProcConfig* config,
                bool        if_not_exists)
{
	// make sure proc does not exists
	auto proc = proc_mgr_find(self, &config->schema, &config->name, false);
	if (proc)
	{
		if (! if_not_exists)
			error("procedure '%.*s': already exists", str_size(&config->name),
			      str_of(&config->name));
		return;
	}

	// allocate proc and init
	proc = proc_allocate(config, self->iface, self->iface_arg);

	// save create operation
	auto op = proc_op_create(config);

	// update mgr
	relation_mgr_create(&self->mgr, tr, CMD_PROC_CREATE, &proc->rel, op);

	// prepare function
	proc_prepare(proc);
}

void
proc_mgr_drop_of(ProcMgr* self, Tr* tr, Proc* proc)
{
	// save drop operation
	auto op = proc_op_drop(&proc->config->schema, &proc->config->name);

	// update mgr
	relation_mgr_drop(&self->mgr, tr, CMD_PROC_DROP, &proc->rel, op);
}

void
proc_mgr_drop(ProcMgr* self,
              Tr*      tr,
              Str*     schema,
              Str*     name,
              bool     if_exists)
{
	auto proc = proc_mgr_find(self, schema, name, false);
	if (! proc)
	{
		if (! if_exists)
			error("procedure '%.*s': not exists", str_size(name),
			      str_of(name));
		return;
	}
	proc_mgr_drop_of(self, tr, proc);
}

static void
rename_if_commit(Log* self, LogOp* op)
{
	buf_free(log_relation_of(self, op)->data);
}

static void
rename_if_abort(Log* self, LogOp* op)
{
	auto relation = log_relation_of(self, op);
	auto proc = proc_of(relation->relation);
	uint8_t* pos = relation->data->start;
	Str schema;
	Str name;
	Str schema_new;
	Str name_new;
	proc_op_rename_read(&pos, &schema, &name, &schema_new, &name_new);
	proc_config_set_schema(proc->config, &schema);
	proc_config_set_name(proc->config, &name);
	buf_free(relation->data);
}

static LogIf rename_if =
{
	.commit = rename_if_commit,
	.abort  = rename_if_abort
};

void
proc_mgr_rename(ProcMgr* self,
                Tr*      tr,
                Str*     schema,
                Str*     name,
                Str*     schema_new,
                Str*     name_new,
                bool     if_exists)
{
	auto proc = proc_mgr_find(self, schema, name, false);
	if (! proc)
	{
		if (! if_exists)
			error("procedure '%.*s': not exists", str_size(name),
			      str_of(name));
		return;
	}

	// ensure new proc does not exists
	if (proc_mgr_find(self, schema_new, name_new, false))
		error("procedure '%.*s': already exists", str_size(name_new),
		      str_of(name_new));

	// save rename operation
	auto op = proc_op_rename(schema, name, schema_new, name_new);

	// update mgr
	log_relation(&tr->log, CMD_PROC_RENAME, &rename_if,
	             NULL,
	             &proc->rel, op);

	// set new name
	if (! str_compare(&proc->config->schema, schema_new))
		proc_config_set_schema(proc->config, schema_new);

	if (! str_compare(&proc->config->name, name_new))
		proc_config_set_name(proc->config, name_new);
}

void
proc_mgr_dump(ProcMgr* self, Buf* buf)
{
	// array
	encode_array(buf);
	list_foreach(&self->mgr.list)
	{
		auto proc = proc_of(list_at(Relation, link));
		proc_config_write(proc->config, buf);
	}
	encode_array_end(buf);
}

Buf*
proc_mgr_list(ProcMgr* self, Str* schema, Str* name, bool extended)
{
	unused(extended);
	auto buf = buf_create();
	if (schema && name)
	{
		// show procedure
		auto proc = proc_mgr_find(self, schema, name, false);
		if (proc) {
			proc_config_write(proc->config, buf);
		} else {
			encode_null(buf);
		}
		return buf;
	}

	// show procedures
	encode_array(buf);
	list_foreach(&self->mgr.list)
	{
		auto proc = proc_of(list_at(Relation, link));
		if (schema && !str_compare(&proc->config->schema, schema))
			continue;
		proc_config_write(proc->config, buf);
	}
	encode_array_end(buf);
	return buf;
}

Proc*
proc_mgr_find(ProcMgr* self, Str* schema, Str* name,
              bool     error_if_not_exists)
{
	auto relation = relation_mgr_get(&self->mgr, schema, name);
	if (! relation)
	{
		if (error_if_not_exists)
			error("procedure '%.*s': not exists", str_size(name),
			      str_of(name));
		return NULL;
	}
	return proc_of(relation);
}

Proc*
proc_mgr_find_dep(ProcMgr* self, Str* schema, Str* name)
{
	list_foreach(&self->mgr.list)
	{
		auto proc = proc_of(list_at(Relation, link));
		if (proc_depend(proc, schema, name))
			return proc;
	}
	return NULL;
}
