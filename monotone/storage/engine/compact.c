
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
#include <monotone_schema.h>
#include <monotone_mvcc.h>
#include <monotone_engine.h>

hot static void
compact_repartition(CompactReq* self, Commit* list)
{
	auto row = list->list;
	while (row)
	{
		auto next = row->next_commit;

		Part* part;
		int partition_id = row_partition(row);
		if (partition_id < self->r->range_start)
			part = self->l;
		else
			part = self->r;

		// update heap
		Row* prev;
		if (row->is_delete)
			prev = heap_delete(&part->heap, row);
		else
			prev = heap_set(&part->heap, row);

		// add to the new commit list and free previous version
		row->next_commit = NULL;
		row->prev_commit = NULL;
		heap_commit(&part->heap, row, prev, list->lsn);

		row = next;
	}

	commit_init(list);
}

static void
compact_split_1(CompactReq* self)
{
	auto origin = self->part;

	// create new partitions
	self->l = part_allocate(origin->schema,
	                        origin->heap.uuid,
	                        origin->id,
	                        config_psn_next(),
	                        origin->range_start,
	                       (origin->range_start + origin->range_end) / 2);

	self->r = part_allocate(origin->schema,
	                        origin->heap.uuid,
	                        origin->id,
	                        config_psn_next(),
	                        self->l->range_end,
	                        origin->range_end);

	// redistribute rows between two partitions
	compact_repartition(self, &self->commit->commit_1);
}

static void
compact_split_2(CompactReq* self)
{
	// finilize redistribution using second commit list
	compact_repartition(self, &self->commit->commit_2);
}

static void
compact_rpc(Rpc* rpc, void* arg)
{
	unused(arg);
	switch (rpc->id) {
	case RPC_COMPACT:
	{
		CompactReq* req = rpc_arg_ptr(rpc, 0);
		switch (req->op) {
		case COMPACT_SPLIT_1:
			compact_split_1(req);
			break;
		case COMPACT_SPLIT_2:
			compact_split_2(req);
			break;
		case COMPACT_SNAPSHOT:
			// todo
			break;
		default:
			break;
		}
		break;
	}
	case RPC_STOP:
		break;
	}
}

static void
compact_main(void *arg)
{
	Compact* self = arg;

	bool stop = false;
	while (! stop)
	{
		auto buf = channel_read(&mn_task->channel, -1);
		auto msg = msg_of(buf);

		// execute command
		stop = msg->id == RPC_STOP;
		rpc_execute(buf, compact_rpc, self);
		buf_free(buf);
	}
}

void
compact_init(Compact* self)
{
	task_init(&self->task, mn_task->buf_cache);
}

void
compact_free(Compact* self)
{
	task_free(&self->task);
}

void
compact_start(Compact* self)
{
	task_create(&self->task, "compaction", compact_main, self);
}

void
compact_stop(Compact* self)
{
	rpc(&self->task.channel, RPC_STOP, 0);
	task_wait(&self->task);
}

void
compact_run(Compact* self, CompactReq* req)
{
	rpc(&self->task.channel, RPC_COMPACT, 1, req);
}
