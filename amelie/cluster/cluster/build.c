
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
#include <sonata_user.h>
#include <sonata_http.h>
#include <sonata_client.h>
#include <sonata_server.h>
#include <sonata_row.h>
#include <sonata_transaction.h>
#include <sonata_index.h>
#include <sonata_partition.h>
#include <sonata_wal.h>
#include <sonata_db.h>
#include <sonata_value.h>
#include <sonata_aggr.h>
#include <sonata_executor.h>
#include <sonata_vm.h>
#include <sonata_parser.h>
#include <sonata_planner.h>
#include <sonata_compiler.h>
#include <sonata_backup.h>
#include <sonata_repl.h>
#include <sonata_cluster.h>

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
	auto buf = msg_begin(RPC_BUILD);
	buf_write(buf, &self, sizeof(Build**));
	msg_end(buf);
	channel_write(route->channel, buf);

	// wait for completion
	buf = channel_read(&self->channel, -1);
	auto msg = msg_of(buf);
	guard(buf_free, buf);

	if (msg->id == MSG_ERROR)
		msg_error_throw(buf);
}

static inline void
build_run_all(Build* self)
{
	// ask each node to build related partition
	list_foreach(&self->cluster->list)
	{
		auto node = list_at(Node, link);
		auto buf = msg_begin(RPC_BUILD);
		buf_write(buf, &self, sizeof(Build**));
		msg_end(buf);
		channel_write(&node->task.channel, buf);
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
		guard_buf(error);
		msg_error_throw(error);
	}
}

void
build_run(Build* self)
{
	channel_attach(&self->channel);

	// run on first node (for reference table) or use whole cluster
	if (self->table && self->table->config->reference)
		build_run_first(self);
	else
		build_run_all(self);
}

void
build_execute(Build* self, Uuid* node)
{
	Exception e;
	if (enter(&e))
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
			PartBuild pb;
			part_build_init(&pb, PART_BUILD_INDEX, part, NULL, NULL, self->index);
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
			PartBuild pb;
			part_build_init(&pb, PART_BUILD_COLUMN_ADD, part, part_dest, self->column, NULL);
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
			PartBuild pb;
			part_build_init(&pb, PART_BUILD_COLUMN_DROP, part, part_dest, self->column, NULL);
			part_build(&pb);
			break;
		}
		case BUILD_NONE:
			break;
		}
	}
	Buf* buf;
	if (leave(&e)) {
		buf = msg_error(&so_self()->error);
	} else {
		buf = msg_begin(MSG_OK);
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
	guard(build_free, &build);
	build_run(&build);
}

static void
build_if_column_add(Recover* self, Table* table, Table* table_new, Column* column)
{
	// rebuild new table with new column in parallel per node
	Cluster* cluster = self->iface_arg;
	Build build;
	build_init(&build, BUILD_COLUMN_ADD, cluster, table, table_new, column, NULL);
	guard(build_free, &build);
	build_run(&build);
}

static void
build_if_column_drop(Recover* self, Table* table, Table* table_new, Column* column)
{
	// rebuild new table without column in parallel per node
	Cluster* cluster = self->iface_arg;
	Build build;
	build_init(&build, BUILD_COLUMN_DROP, cluster, table, table_new, column, NULL);
	guard(build_free, &build);
	build_run(&build);
}

RecoverIf build_if =
{
	.build_index       = build_if_index,
	.build_column_add  = build_if_column_add,
	.build_column_drop = build_if_column_drop
};
