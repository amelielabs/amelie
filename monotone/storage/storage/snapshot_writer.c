
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
#include <monotone_index.h>
#include <monotone_storage.h>

static void
snapshot_writer_write(Rpc* rpc, void* arg)
{
	unused(arg);
	Snapshot* snapshot = rpc_arg_ptr(rpc, 0);
	snapshot_create(snapshot);
}

static void
snapshot_writer_main(void* arg)
{
	unused(arg);
	for (;;)
	{
		auto buf = channel_read(&mn_task->channel, -1);
		auto msg = msg_of(buf);
		guard(buf_guard, buf_free, buf);
		if (unlikely(msg->id == RPC_STOP))
		{
			rpc_done(rpc_of(buf));
			break;
		}
		rpc_execute(buf, snapshot_writer_write, NULL);
	}
}

void
snapshot_writer_init(SnapshotWriter* self)
{
	task_init(&self->task);
}

void
snapshot_writer_start(SnapshotWriter* self)
{
	task_create(&self->task, "snapshot_writer", snapshot_writer_main, self);
}

void
snapshot_writer_stop(SnapshotWriter* self)
{
	rpc(&self->task.channel, RPC_STOP, 0);
	task_wait(&self->task);
	task_free(&self->task);
}

void
snapshot_write(SnapshotWriter* self, Snapshot* snapshot)
{
	rpc(&self->task.channel, RPC_SNAPSHOT_WRITE, 1, snapshot);
}
