
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
#include <amelie_server>
#include <amelie_storage>
#include <amelie_repl>
#include <amelie_vm>
#include <amelie_frontend.h>
#include <amelie_backend.h>

void
build_init(Build* self)
{
	self->config   = NULL;
	self->dispatch = NULL;
	self->program  = program_allocate();
	local_init(&self->local);
	dtr_init(&self->dtr, &self->local);
}

void
build_free(Build* self)
{
	dtr_free(&self->dtr);
	program_free(self->program);
}

void
build_reset(Build* self)
{
	self->config   = NULL;
	self->dispatch = NULL;
	dtr_reset(&self->dtr);
}

void
build_prepare(Build* self, BuildConfig* config)
{
	// set config
	self->config = config;

	// prepare distributed transaction
	auto dtr = &self->dtr;
	dtr_create(&self->dtr, self->program);

	// prepare dispatch
	auto dispatch_mgr = &dtr->dispatch_mgr;
	auto dispatch = dispatch_create(&dispatch_mgr->cache);
	dispatch_set_close(dispatch);
	self->dispatch = dispatch;
}

void
build_add(Build* self, Part* part)
{
	auto dispatch_mgr = &self->dtr.dispatch_mgr;
	auto req = dispatch_add(self->dispatch, &dispatch_mgr->cache_req,
	                        REQ_BUILD, -1, NULL, NULL,
	                        part);
	buf_write(&req->arg, &self, sizeof(Build*));
}

void
build_add_all(Build* self, Storage* storage)
{
	// add all partitions
	list_foreach(&storage->catalog.table_mgr.mgr.list)
	{
		auto table = table_of(list_at(Relation, link));
		list_foreach(&table->part_list.list)
		{
			auto part = list_at(Part, link);
			build_add(self, part);
		}
	}
}

void
build_run(Build* self)
{
	assert(self->dispatch);
	if (! self->dispatch->list_count)
	{
		auto dispatch_mgr = &self->dtr.dispatch_mgr;
		dispatch_cache_push(&dispatch_mgr->cache, self->dispatch, &dispatch_mgr->cache_req);
		self->dispatch = NULL;
		return;
	}

	auto config = self->config;
	switch (config->type) {
	case BUILD_INDEX:
	{
		auto table_config = config->table->config;
		info("");
		info("alter: create index %.*s on %.*s",
		     str_size(&config->index->name),
		     str_of(&config->index->name),
		     str_size(&table_config->name),
		     str_of(&table_config->name));
		break;
	}
	case BUILD_COLUMN_ADD:
	{
		auto table_config = config->table->config;
		info("");
		info("alter: alter table %.*s add column %.*s",
		     str_size(&table_config->name),
		     str_of(&table_config->name),
		     str_size(&config->column->name),
		     str_of(&config->column->name));
		break;
	}
	case BUILD_COLUMN_DROP:
	{
		auto table_config = config->table->config;
		info("");
		info("alter: alter table %.*s drop column %.*s",
		     str_size(&table_config->name),
		     str_of(&table_config->name),
		     str_size(&config->column->name),
		     str_of(&config->column->name));
		break;
	}
	// BUILD_RECOVER
	// BUILD_NONE
	default:
		break;
	}

	// execute
	auto dtr = &self->dtr;
	auto dispatch = self->dispatch;
	auto on_error = error_catch(
		executor_send(share()->executor, dtr, dispatch);
	);
	Buf* error = NULL;
	if (on_error)
		error = error_create(&am_self()->error);
	commit(share()->commit, dtr, error);

	if (config->type != BUILD_RECOVER &&
	    config->type != BUILD_NONE)
		info(" ");
}

void
build_execute(Build* self, Part* part)
{
	auto config = self->config;
	switch (config->type) {
	case BUILD_RECOVER:
		recover_part(part);
		break;
	case BUILD_INDEX:
	{
		// build new index content for current worker
		auto table_config = config->table->config;
		PartBuild pb;
		part_build_init(&pb, PART_BUILD_INDEX, part, NULL, NULL,
		                config->index,
		                &table_config->db, &table_config->name);
		part_build(&pb);
		break;
	}
	case BUILD_COLUMN_ADD:
	{
		auto part_dest = part_list_find(&config->table_new->part_list, part->config->id);
		assert(part_dest);

		// build new table with new column for current worker
		auto table_config = config->table->config;
		PartBuild pb;
		part_build_init(&pb, PART_BUILD_COLUMN_ADD, part, part_dest,
		                config->column, NULL,
		                &table_config->db, &table_config->name);
		part_build(&pb);
		break;
	}
	case BUILD_COLUMN_DROP:
	{
		auto part_dest = part_list_find(&config->table_new->part_list, part->config->id);
		assert(part_dest);

		// build new table without column for current worker
		auto table_config = config->table->config;
		PartBuild pb;
		part_build_init(&pb, PART_BUILD_COLUMN_DROP, part, part_dest,
		                config->column, NULL,
		                &table_config->db, &table_config->name);
		part_build(&pb);
		break;
	}
	case BUILD_NONE:
		// do nothing, used for sync
		break;
	}
}
