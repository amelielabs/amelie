
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
#include <monotone_auth.h>
#include <monotone_client.h>
#include <monotone_server.h>
#include <monotone_schema.h>
#include <monotone_transaction.h>
#include <monotone_storage.h>
#include <monotone_wal.h>
#include <monotone_db.h>
#include <monotone_shard.h>

static inline Storage*
storage_list_first(StorageList* self)
{
	list_foreach(&self->list)
	{
		auto storage = list_at(Storage, link_list);
		return storage;
	}
	return NULL;
}

static void
shard_request(Shard* self, Request* req)
{
	unused(self);

	auto ro = req->ro;
	auto on_commit = condition_create();
	req->on_commit = on_commit;

	// execute
	transaction_begin(&req->trx);

	auto storage = storage_list_first(&self->storage_list);
	uint8_t* pos = req->buf->start;
	int i = 0;
	for (; i < req->buf_count; i++)
	{
		uint8_t* start = pos;
		data_skip(&pos);

		storage_write(storage, &req->trx, LOG_REPLACE, false, start, pos - start);
	}

	// prepare wal write
	if (! ro)
		wal_record_create(&req->wal_record, &req->trx.log);

	// OK
	auto reply = msg_create(MSG_OK);
	encode_integer(reply, false);
	msg_end(reply);
	channel_write(&req->src, reply);

	// wait for commit
	if (! ro)
		condition_wait(on_commit, -1);

	transaction_commit(&req->trx);
	condition_free(on_commit);
}

static void
shard_rpc(Rpc* rpc, void* arg)
{
	Shard* self = arg;
	switch (rpc->id) {
	case RPC_STORAGE_ATTACH:
	{
		Storage* storage = rpc_arg_ptr(rpc, 0);
		storage_list_add(&self->storage_list, storage);
		break;
	}
	case RPC_STORAGE_DETACH:
		break;
	case RPC_STOP:
		break;
	default:
		break;
	}
}

static void
shard_main(void* arg)
{
	Shard* self = arg;
	bool stop = false;
	while (! stop)
	{
		auto buf = channel_read(&mn_task->channel, -1);
		auto msg = msg_of(buf);
		guard(buf_guard, buf_free, buf);

		if (msg->id == RPC_REQUEST)
		{
			// request
			auto req = request_of(buf);
			shard_request(self, req);
		} else
		{
			// rpc
			stop = msg->id == RPC_STOP;
			rpc_execute(buf, shard_rpc, self);
		}
	}
}

Shard*
shard_allocate(ShardConfig* config)
{
	Shard* self = mn_malloc(sizeof(*self));
	self->order = 0;
	task_init(&self->task, mn_task->buf_cache);
	storage_list_init(&self->storage_list);
	guard(self_guard, shard_free, self);
	self->config = shard_config_copy(config);
	return unguard(&self_guard);
}

void
shard_free(Shard* self)
{
	if (self->config)
		shard_config_free(self->config);
	mn_free(self);
}

void
shard_start(Shard* self)
{
	task_create(&self->task, "shard", shard_main, self);
}

void
shard_stop(Shard* self)
{
	// send stop request
	if (task_active(&self->task))
	{
		rpc(&self->task.channel, RPC_STOP, 0);
		task_wait(&self->task);
		task_free(&self->task);
		task_init(&self->task, mn_task->buf_cache);
	}
}
