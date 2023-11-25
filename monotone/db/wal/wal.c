
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
#include <monotone_snapshot.h>
#include <monotone_wal.h>

hot static void
wal_rpc(Rpc* rpc, void* arg)
{
	unused(rpc);
	Wal* self = arg;
	switch (rpc->id) {
	case RPC_WAL_GC:
	{
		uint64_t snapshot = rpc_arg(rpc, 0);
		wal_store_gc(&self->wal_store, snapshot);
		break;
	}
	case RPC_WAL_SNAPSHOT:
	{
		WalSnapshot* snapshot = rpc_arg_ptr(rpc, 0);
		wal_store_snapshot(&self->wal_store, snapshot);
		break;
	}
	case RPC_WAL_WRITE:
	{
		LogSet* set = rpc_arg_ptr(rpc, 0);
		wal_store_write(&self->wal_store, set);
		break;
	}
	case RPC_WAL_STATUS:
	{
		Buf** buf = rpc_arg_ptr(rpc, 0);
		*buf = wal_store_status(&self->wal_store);
		break;
	}
	default:
		break;
	}
}

static void
wal_main(void *arg)
{
	Wal* self = arg;
	bool stop = false;
	while (! stop)
	{
		auto buf = channel_read(&mn_task->channel, -1);
		auto msg = msg_of(buf);

		// command
		stop = msg->id == RPC_STOP;
		rpc_execute(buf, wal_rpc, self);
		buf_free(buf);
	}
}

void
wal_init(Wal* self)
{
	wal_store_init(&self->wal_store);
	task_init(&self->task);
}

void
wal_free(Wal* self)
{
	wal_store_free(&self->wal_store);
}

void
wal_open(Wal* self)
{
	wal_store_open(&self->wal_store);
}

void
wal_start(Wal* self)
{
	task_create(&self->task, "wal", wal_main, self);
}

void
wal_stop(Wal* self)
{
	if (task_active(&self->task))
	{
		rpc(&self->task.channel, RPC_STOP, 0);
		task_wait(&self->task);
		task_free(&self->task);
		task_init(&self->task);
	}
}

void
wal_gc(Wal* self, uint64_t snapshot)
{
	rpc(&self->task.channel, RPC_WAL_GC, 1, snapshot);
}

void
wal_snapshot(Wal* self, WalSnapshot* snapshot)
{
	rpc(&self->task.channel, RPC_WAL_SNAPSHOT, 1, snapshot);
}

void
wal_write(Wal* self, LogSet* set)
{
	rpc(&self->task.channel, RPC_WAL_WRITE, 1, set);
}

Buf*
wal_status(Wal* self)
{
	Buf* buf = NULL;
	rpc(&self->task.channel, RPC_WAL_STATUS, 1, &buf);
	return buf;
}
