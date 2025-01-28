
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
#include <amelie_planner.h>
#include <amelie_compiler.h>
#include <amelie_cluster.h>

void
build_init(Build*       self,
           BuildType    type,
           Cluster*     cluster,
           Table*       table,
           Table*       table_new,
           Column*      column,
           IndexConfig* index)
{
	self->type      = type;
	self->table     = table;
	self->table_new = table_new;
	self->column    = column;
	self->index     = index;
	self->cluster   = cluster;
	channel_init(&self->channel);
}

void
build_free(Build* self)
{
	channel_detach(&self->channel);
	channel_free(&self->channel);
}

static inline void
build_run_first(Build* self)
{
	// ask first node to build related partition
	auto route = router_first(&self->cluster->router);
	auto buf = msg_create(RPC_BUILD);
	buf_write(buf, &self, sizeof(Build**));
	msg_end(buf);
	channel_write(route->channel, buf);

	// wait for completion
	buf = channel_read(&self->channel, -1);
	defer_buf(buf);

	auto msg = msg_of(buf);
	if (msg->id == MSG_ERROR)
		msg_error_throw(buf);
}

static inline void
build_run_all(Build* self)
{
	// ask each node to build related partition
	list_foreach(&self->cluster->list)
	{
		auto compute = list_at(Compute, link);
		auto buf = msg_create(RPC_BUILD);
		buf_write(buf, &self, sizeof(Build**));
		msg_end(buf);
		channel_write(&compute->task.channel, buf);
	}

	// wait for completion
	Buf* error = NULL;
	int  complete;
	for (complete = 0; complete < self->cluster->list_count;
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
}

void
build_run(Build* self)
{
	channel_attach(&self->channel);

	// run on first node (for shared table) or use whole cluster
	if (self->table && self->table->config->shared)
		build_run_first(self);
	else
		build_run_all(self);
}

static void
build_execute_op(Build* self, Uuid* node)
{
	switch (self->type) {
	case BUILD_RECOVER:
		// restore last checkpoint partitions related to the node
		recover_checkpoint(self->cluster->db, node);
		break;
	case BUILD_INDEX:
	{
		auto part = part_list_match(&self->table->part_list, node);
		if (! part)
			break;
		// build new index content for current node
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
		auto part = part_list_match(&self->table->part_list, node);
		if (! part)
			break;
		auto part_dest = part_list_match(&self->table_new->part_list, node);
		assert(part_dest);
		// build new table with new column for current node
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
		auto part = part_list_match(&self->table->part_list, node);
		if (! part)
			break;
		auto part_dest = part_list_match(&self->table_new->part_list, node);
		assert(part_dest);
		// build new table without column for current node
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
build_execute(Build* self, Uuid* node)
{
	Buf* buf;
	if (error_catch( build_execute_op(self, node) ))
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
	// do parallel indexation per node
	Cluster* cluster = self->iface_arg;
	Build build;
	build_init(&build, BUILD_INDEX, cluster, table, NULL, NULL, index);
	defer(build_free, &build);
	build_run(&build);
}

static void
build_if_column_add(Recover* self, Table* table, Table* table_new, Column* column)
{
	// rebuild new table with new column in parallel per node
	Cluster* cluster = self->iface_arg;
	Build build;
	build_init(&build, BUILD_COLUMN_ADD, cluster, table, table_new, column, NULL);
	defer(build_free, &build);
	build_run(&build);
}

static void
build_if_column_drop(Recover* self, Table* table, Table* table_new, Column* column)
{
	// rebuild new table without column in parallel per node
	Cluster* cluster = self->iface_arg;
	Build build;
	build_init(&build, BUILD_COLUMN_DROP, cluster, table, table_new, column, NULL);
	defer(build_free, &build);
	build_run(&build);
}

RecoverIf build_if =
{
	.build_index       = build_if_index,
	.build_column_add  = build_if_column_add,
	.build_column_drop = build_if_column_drop
};
