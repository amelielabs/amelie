
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

#include <amelie_base.h>
#include <amelie_os.h>
#include <amelie_lib.h>
#include <amelie_json.h>
#include <amelie_runtime.h>
#include <amelie_row.h>
#include <amelie_heap.h>
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_partition.h>
#include <amelie_checkpoint.h>
#include <amelie_catalog.h>

void
proc_mgr_init(ProcMgr* self, ProcFree free, void* free_arg)
{
	self->free     = free;
	self->free_arg = free_arg;
	relation_mgr_init(&self->mgr);
}

void
proc_mgr_free(ProcMgr* self)
{
	relation_mgr_free(&self->mgr);
}

bool
proc_mgr_create(ProcMgr*    self,
                Tr*         tr,
                ProcConfig* config,
                bool        if_not_exists)
{
	// make sure proc does not exists
	auto current = proc_mgr_find(self, &config->schema, &config->name, false);
	if (current)
	{
		if (! if_not_exists)
			error("procedure '%.*s': already exists", str_size(&config->name),
			      str_of(&config->name));
		return false;
	}

	// create procedure
	auto proc = proc_allocate(config, self->free, self->free_arg);
	relation_mgr_create(&self->mgr, tr, &proc->rel);
	return true;
}

void
proc_mgr_drop_of(ProcMgr* self, Tr* tr, Proc* proc)
{
	// drop procedure by object
	relation_mgr_drop(&self->mgr, tr, &proc->rel);
}

bool
proc_mgr_drop(ProcMgr* self, Tr* tr, Str* schema, Str* name,
              bool     if_exists)
{
	auto proc = proc_mgr_find(self, schema, name, false);
	if (! proc)
	{
		if (! if_exists)
			error("procedure '%.*s': not exists", str_size(name),
			      str_of(name));
		return false;
	}
	proc_mgr_drop_of(self, tr, proc);
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
	auto proc = proc_of(relation->relation);
	uint8_t* pos = relation->data;
	Str schema;
	Str name;
	json_read_string(&pos, &schema);
	json_read_string(&pos, &name);
	proc_config_set_schema(proc->config, &schema);
	proc_config_set_name(proc->config, &name);
}

static LogIf rename_if =
{
	.commit = rename_if_commit,
	.abort  = rename_if_abort
};

bool
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
		return false;
	}

	// ensure new procedure does not exists
	if (proc_mgr_find(self, schema_new, name_new, false))
		error("procedure '%.*s': already exists", str_size(name_new),
		      str_of(name_new));

	// update procedure
	log_relation(&tr->log, &rename_if, NULL, &proc->rel);

	// save previous name
	encode_string(&tr->log.data, &proc->config->schema);
	encode_string(&tr->log.data, &proc->config->name);

	// set new name
	if (! str_compare(&proc->config->schema, schema_new))
		proc_config_set_schema(proc->config, schema_new);

	if (! str_compare(&proc->config->name, name_new))
		proc_config_set_name(proc->config, name_new);

	return true;
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

Buf*
proc_mgr_list(ProcMgr* self, Str* schema, Str* name, bool extended)
{
	auto buf = buf_create();
	if (schema && name)
	{
		// show procedure
		auto proc = proc_mgr_find(self, schema, name, false);
		if (proc) {
			if (extended)
				proc_config_write(proc->config, buf);
			else
				proc_config_write_compact(proc->config, buf);
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
		if (extended)
			proc_config_write(proc->config, buf);
		else
			proc_config_write_compact(proc->config, buf);
	}
	encode_array_end(buf);
	return buf;
}
