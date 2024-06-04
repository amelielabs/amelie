
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
#include <sonata_def.h>
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
#include <sonata_semantic.h>
#include <sonata_compiler.h>
#include <sonata_backup.h>
#include <sonata_repl.h>
#include <sonata_cluster.h>

void
cluster_recover(Cluster* self)
{
	// recover partitions in parallel
	list_foreach(&self->list)
	{
		auto node = list_at(Node, link);
		auto buf = msg_begin(RPC_RECOVER);
		msg_end(buf);
		channel_write(&node->task.channel, buf);
	}

	// wait for completion
	int complete = 0;
	int errors   = 0;
	while (complete < self->list_count)
	{
		auto buf = channel_read(&so_task->channel, -1);
		auto msg = msg_of(buf);
		guard(buf_free, buf);
		complete++;
		if (msg->id == MSG_ERROR)
			errors++;
	}
	if (errors > 0)
		error("recovery: failed");

	// replay wals
	recover_wal(self->db);
}
