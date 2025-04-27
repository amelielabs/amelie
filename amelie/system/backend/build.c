
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
#include <amelie_user.h>
#include <amelie_auth.h>
#include <amelie_http.h>
#include <amelie_client.h>
#include <amelie_server.h>
#include <amelie_row.h>
#include <amelie_heap.h>
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_partition.h>
#include <amelie_checkpoint.h>
#include <amelie_wal.h>
#include <amelie_db.h>
#include <amelie_backup.h>
#include <amelie_repl.h>
#include <amelie_value.h>
#include <amelie_set.h>
#include <amelie_content.h>
#include <amelie_executor.h>
#include <amelie_func.h>
#include <amelie_vm.h>
#include <amelie_parser.h>
#include <amelie_compiler.h>
#include <amelie_frontend.h>
#include <amelie_backend.h>

void
build_init(Build*       self,
           BuildType    type,
           BackendMgr*  backend_mgr,
           Table*       table,
           Table*       table_new,
           Column*      column,
           IndexConfig* index)
{
	self->type        = type;
	self->table       = table;
	self->table_new   = table_new;
	self->column      = column;
	self->index       = index;
	self->backend_mgr = backend_mgr;
	channel_init(&self->channel);
}

void
build_free(Build* self)
{
	channel_detach(&self->channel);
	channel_free(&self->channel);
}

void
build_run(Build* self)
{
	switch (self->type) {
	case BUILD_INDEX:
	{
		auto config = self->table->config;
		info("");
		info("⟶ create index %.*s on %.*s.%.*s",
		     str_size(&self->index->name),
		     str_of(&self->index->name),
		     str_size(&config->schema),
		     str_of(&config->schema),
		     str_size(&config->name),
		     str_of(&config->name));
		break;
	}
	case BUILD_COLUMN_ADD:
	{
		auto config = self->table->config;
		info("");
		info("⟶ alter table %.*s.%.*s add column %.*s",
		     str_size(&config->schema),
		     str_of(&config->schema),
		     str_size(&config->name),
		     str_of(&config->name),
		     str_size(&self->column->name),
		     str_of(&self->column->name));
		break;
	}
	case BUILD_COLUMN_DROP:
	{
		auto config = self->table->config;
		info("");
		info("⟶ alter table %.*s.%.*s drop column %.*s",
		     str_size(&config->schema),
		     str_of(&config->schema),
		     str_size(&config->name),
		     str_of(&config->name),
		     str_size(&self->column->name),
		     str_of(&self->column->name));
		break;
	}
	// BUILD_RECOVER
	// BUILD_NONE
	default:
		break;
	}

	channel_attach(&self->channel);

	// ask each worker to build related partition
	auto backend_mgr = self->backend_mgr;
	for (auto i = 0; i < backend_mgr->workers_count; i++)
	{
		auto backend = backend_mgr->workers[i];
		auto buf = msg_create(RPC_BUILD);
		buf_write(buf, &self, sizeof(Build**));
		msg_end(buf);
		channel_write(&backend->task.channel, buf);
	}

	// wait for completion
	Buf* error = NULL;
	int  complete;
	for (complete = 0; complete < backend_mgr->workers_count;
	     complete++)
	{
		auto buf = channel_read(&self->channel, -1);
		auto msg = msg_of(buf);
		if (msg->id == MSG_ERROR && !error)
		{
			error = buf;
			continue;
		}
		buf_free(buf);
	}
	if (error)
	{
		defer_buf(error);
		msg_error_throw(error);
	}

	info("complete");
}

static void
build_execute_op(Build* self, Route* worker)
{
	switch (self->type) {
	case BUILD_RECOVER:
		// restore last checkpoint partitions related to the worker
		recover_checkpoint(self->backend_mgr->db, worker);
		break;
	case BUILD_INDEX:
	{
		auto part = part_list_match(&self->table->part_list, worker);
		if (! part)
			break;
		// build new index content for current worker
		auto config = self->table->config;
		PartBuild pb;
		part_build_init(&pb, PART_BUILD_INDEX, part, NULL, NULL,
		                self->index,
		                &config->schema, &config->name);
		part_build(&pb);
		break;
	}
	case BUILD_COLUMN_ADD:
	{
		auto part = part_list_match(&self->table->part_list, worker);
		if (! part)
			break;
		auto part_dest = part_list_match(&self->table_new->part_list, worker);
		assert(part_dest);
		// build new table with new column for current worker
		auto config = self->table->config;
		PartBuild pb;
		part_build_init(&pb, PART_BUILD_COLUMN_ADD, part, part_dest, self->column,
		                NULL,
		                &config->schema, &config->name);
		part_build(&pb);
		break;
	}
	case BUILD_COLUMN_DROP:
	{
		auto part = part_list_match(&self->table->part_list, worker);
		if (! part)
			break;
		auto part_dest = part_list_match(&self->table_new->part_list, worker);
		assert(part_dest);
		// build new table without column for current worker
		auto config = self->table->config;
		PartBuild pb;
		part_build_init(&pb, PART_BUILD_COLUMN_DROP, part, part_dest, self->column,
		                NULL,
		                &config->schema, &config->name);
		part_build(&pb);
		break;
	}
	case BUILD_NONE:
		break;
	}
}

void
build_execute(Build* self, Route* worker)
{
	Buf* buf;
	if (error_catch( build_execute_op(self, worker) ))
	{
		buf = msg_error(&am_self()->error);
	} else
	{
		buf = msg_create(MSG_OK);
		msg_end(buf);
	}
	channel_write(&self->channel, buf);
}

static void
build_if_index(Recover* self, Table* table, IndexConfig* index)
{
	// do parallel indexation per worker
	BackendMgr* backend_mgr = self->iface_arg;
	Build build;
	build_init(&build, BUILD_INDEX, backend_mgr, table, NULL, NULL, index);
	defer(build_free, &build);
	build_run(&build);
}

static void
build_if_column_add(Recover* self, Table* table, Table* table_new, Column* column)
{
	// rebuild new table with new column in parallel per worker
	BackendMgr* backend_mgr = self->iface_arg;
	Build build;
	build_init(&build, BUILD_COLUMN_ADD, backend_mgr, table, table_new, column, NULL);
	defer(build_free, &build);
	build_run(&build);
}

static void
build_if_column_drop(Recover* self, Table* table, Table* table_new, Column* column)
{
	// rebuild new table without column in parallel per worker
	BackendMgr* backend_mgr = self->iface_arg;
	Build build;
	build_init(&build, BUILD_COLUMN_DROP, backend_mgr, table, table_new, column, NULL);
	defer(build_free, &build);
	build_run(&build);
}

RecoverIf build_if =
{
	.build_index       = build_if_index,
	.build_column_add  = build_if_column_add,
	.build_column_drop = build_if_column_drop
};
