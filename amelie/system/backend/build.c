
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
	local_init(&self->local, global());
	dtr_init(&self->dtr, &self->local, &self->backend_mgr->core_mgr);
}

void
build_free(Build* self)
{
	dtr_free(&self->dtr);
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

	auto backend_mgr = self->backend_mgr;
	auto dtr = &self->dtr;

	// ask each backend to build related partition
	ReqList req_list;
	req_list_init(&req_list);
	for (auto i = 0; i < backend_mgr->workers_count; i++)
	{
		auto backend = backend_mgr->workers[i];
		auto req = req_create(&dtr->dispatch_mgr.req_cache);
		req->type = REQ_BUILD;
		req->core = &backend->core;
		buf_write(&req->arg, &self, sizeof(Build*));
		req_list_add(&req_list, req);
	}

	// process distributed transaction
	auto program = program_allocate();
	defer(program_free, program);
	program->sends = 1;

	dtr_reset(dtr);
	dtr_create(dtr, program);

	auto executor = self->backend_mgr->executor;
	auto on_error = error_catch
	(
		executor_send(executor, dtr, &req_list);
		executor_recv(executor, dtr);
	);
	Buf* error = NULL;
	if (on_error)
		error = msg_error(&am_self()->error);
	executor_commit(executor, dtr, error);

	info("complete");
}

void
build_execute(Build* self, Core* worker)
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
		// do nothing, used for sync
		break;
	}
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
