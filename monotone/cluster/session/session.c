
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
#include <monotone_mvcc.h>
#include <monotone_storage.h>
#include <monotone_db.h>
#include <monotone_shard.h>
#include <monotone_session.h>

void
session_init(Session* self, Share* share, Portal* portal)
{
	self->share      = share;
	self->portal     = portal;
	self->cat_locker = NULL;
	transaction_init(&self->trx);
	request_set_init(&self->req_set);
	request_cache_init(&self->req_cache);
}

void
session_free(Session *self)
{
	request_set_reset(&self->req_set, &self->req_cache);
	request_set_free(&self->req_set);
	request_cache_free(&self->req_cache);
	transaction_free(&self->trx);
}

static void
create_table(Session* self)
{
	auto trx = &self->trx;
	auto share = self->share;

	// find table
	Str name;
	str_set_cstr(&name, "test");
	auto table = table_mgr_find(self->share->table_mgr, &name, false);
	if (table)
		return;

	// create table
	mvcc_begin(trx);

	// table config
	auto config = table_config_allocate();
	Uuid id;
	uuid_mgr_generate(global()->uuid_mgr, &id);
	table_config_set_id(config, &id);
	str_set_cstr(&name, "test");
	table_config_set_name(config, &name);

	// schema
	auto pk = column_allocate();
	str_set_cstr(&name, "id");
	column_set_name(pk, &name);
	column_set_type(pk, TYPE_INT);
	schema_add_column(&config->schema, pk);
	schema_add_key(&config->schema, pk);

	table_mgr_create(self->share->table_mgr, trx, config, true);
	table_config_free(config);

	//wal_write(&self->db->wal, &trx->log);

	transaction_set_lsn(trx, trx->log.lsn);
	mvcc_commit(trx);

	// find table
	str_set_cstr(&name, "test");
	table = table_mgr_find(self->share->table_mgr, &name, true);

	// create storage for each shard
	for (int i = 0; i < share->shard_mgr->shards_count; i++)
	{
		auto shard = share->shard_mgr->shards[i];

		// storage config
		auto storage_config = storage_config_allocate();
		uuid_mgr_generate(global()->uuid_mgr, &id);
		storage_config_set_id(storage_config, &id);
		storage_config_set_id_table(storage_config, &table->config->id);
		storage_config_set_id_shard(storage_config, &shard->config->id);
		storage_config_set_range(storage_config, shard->config->range_start,
		                         shard->config->range_end);

		// create storage
		auto storage = storage_mgr_create(share->storage_mgr, storage_config);

		// index config
		auto index_config = index_config_allocate();
		str_set_cstr(&name, "primary");
		index_config_set_name(index_config, &name);
		index_config_set_type(index_config, INDEX_TREE);
		index_config_set_primary(index_config, true);
		schema_copy(&index_config->schema, &table->config->schema);

		// create tree index
		auto index = index_tree_allocate(index_config);
		index_config_free(index_config);

		// attach index to the storage
		storage_mgr_attach(share->storage_mgr, storage, index);

		// attach and start storage on shard
		rpc(&shard->task.channel, RPC_STORAGE_ATTACH, 1, storage);
	}
}

static void
create_table_main(Session* self)
{
	RequestLock req_lock;
	request_lock_init(&req_lock);
	req_lock.on_lock = condition_create();

	// RPC_CAT_LOCK_REQ
	channel_write(global()->control->core, request_lock_msg(&req_lock));

	// wait for exclusive lock to be taken on all hubs (including this one)
	condition_wait(req_lock.on_lock, -1);

	// do ddl
	create_table(self);

	// ask core to unlock hubs
	condition_signal(req_lock.on_unlock);
	condition_free(req_lock.on_lock);
}

#if 0
static void
bench(Session* self)
{
	auto trx = &self->trx;
	auto share = self->share;

	// insert
	Str name;
	str_set_cstr(&name, "test");
	auto table = table_mgr_find(share->table_mgr, &name, true);
	unused(table);

	struct
	{
		Request* req;
		Shard*   shard;
		Buf*     buf;
		int      count;
	} q[share->shard_mgr->shards_count];

	int i = 0;
	for (; i < share->shard_mgr->shards_count; i++)
	{
		auto shard = share->shard_mgr->shards[i];
		q[i].req   = NULL;
		q[i].shard = shard;
		q[i].buf   = buf_create(0);
		q[i].count = 0;
	}

	uint64_t time = timer_mgr_gettime();

	int count = 10000000;
	int batch = 1000;
	for (i = 0; i < count / batch; i++)
	{
//		mvcc_begin(trx);

		// prepare
		int k = 0;
		for (k = 0; k < share->shard_mgr->shards_count; k++)
		{
			auto shard = share->shard_mgr->shards[k];

			buf_reset(q[k].buf);
			q[k].count = 0;

			auto req = request_create(&self->req_cache, &shard->task.channel);
			req->ro = false;
			req->trx = trx;
			req->buf = q[k].buf;
			req->buf_count = &q[k].count;

			request_set_add(&self->req_set, req);
		}

		for (int j = 0; j < batch; j++)
		{
			int key = rand();

			uint8_t  key_data[16];
			uint8_t* key_data_pos = key_data;
			data_write_integer(&key_data_pos, key);
			uint32_t key_hash = hash_murmur3_32(key_data, key_data_pos - key_data, 0);
			int part_id = key_hash % PARTITION_MAX;

			for (k = 0; k < share->shard_mgr->shards_count; k++)
			{
				if (part_id >= q[k].shard->config->range_start &&
				    part_id  < q[k].shard->config->range_end)
				{
					encode_array(q[k].buf, 1);
					encode_integer(q[k].buf, key);
					q[k].count++;
					break;
				}
			}
		}

		// execute
		request_sched_lock(share->req_sched);
		request_set_execute(&self->req_set);
		request_sched_unlock(share->req_sched);

		// wait for completion
		request_set_wait(&self->req_set);

		// todo: wal write
//		transaction_set_lsn(trx, trx->log.lsn);

//		mvcc_commit(trx);

		// signal shard on commit
		list_foreach(&self->req_set.list)
		{
			auto req = list_at(Request, link);
			assert(req->on_commit);
			condition_signal(req->on_commit);
		}

		request_set_reset(&self->req_set, &self->req_cache);
	}

	for (i = 0; i < share->shard_mgr->shards_count; i++)
		buf_free(q[i].buf);

	time = (timer_mgr_gettime() - time) / 1000;

	// time ms
	//portal_write(self->portal, make_float((float)time / 1000.0));

	// rps
	double rps = count / (float)(time / 1000.0 / 1000.0);
	portal_write(self->portal, make_float(rps));

}
#endif

int seq = 0;

static void
bench_client(void* arg)
{
	Session* self = arg;
	auto trx = &self->trx;
	auto share = self->share;

	RequestSet req_set;
	RequestCache req_cache;

	request_set_init(&req_set);
	request_cache_init(&req_cache);

	// insert
	struct
	{
		Request* req;
		Shard*   shard;
		Buf*     buf;
		int      count;
	} q[share->shard_mgr->shards_count];

	int i = 0;
	for (; i < share->shard_mgr->shards_count; i++)
	{
		auto shard = share->shard_mgr->shards[i];
		q[i].req   = NULL;
		q[i].shard = shard;
		q[i].buf   = buf_create(0);
		q[i].count = 0;
	}

	int count = 10000000 / 10;
	int batch = 1000;
	for (i = 0; i < count / batch; i++)
	{
//		mvcc_begin(trx);

		// prepare
		int k = 0;
		for (k = 0; k < share->shard_mgr->shards_count; k++)
		{
			auto shard = share->shard_mgr->shards[k];

			buf_reset(q[k].buf);
			q[k].count = 0;

			auto req = request_create(&req_cache, &shard->task.channel);
			req->ro = false;
			req->trx = trx;
			req->buf = q[k].buf;
			req->buf_count = &q[k].count;

			request_set_add(&req_set, req);
		}

		for (int j = 0; j < batch; j++)
		{
			//int key = rand();
			int key = seq++;

			uint8_t  key_data[16];
			uint8_t* key_data_pos = key_data;
			data_write_integer(&key_data_pos, key);
			uint32_t key_hash = hash_murmur3_32(key_data, key_data_pos - key_data, 0);
			int part_id = key_hash % PARTITION_MAX;

			for (k = 0; k < share->shard_mgr->shards_count; k++)
			{
				if (part_id >= q[k].shard->config->range_start &&
				    part_id  < q[k].shard->config->range_end)
				{
					encode_array(q[k].buf, 1);
					encode_integer(q[k].buf, key);
					q[k].count++;
					break;
				}
			}
		}

		// execute
		request_sched_lock(share->req_sched);
		request_set_execute(&req_set);
		request_sched_unlock(share->req_sched);

		// wait for completion
		request_set_wait(&req_set);

		// todo: wal write
//		transaction_set_lsn(trx, trx->log.lsn);
//		mvcc_commit(trx);

		// signal shard on commit
		list_foreach(&req_set.list)
		{
			auto req = list_at(Request, link);
			assert(req->on_commit);
			condition_signal(req->on_commit);
		}

		request_set_reset(&req_set, &req_cache);
	}

	for (i = 0; i < share->shard_mgr->shards_count; i++)
		buf_free(q[i].buf);

	request_set_free(&req_set);
	request_cache_free(&req_cache);
}

hot static inline void
execute(Session* self, Buf* buf)
{
	//auto share = self->share;
	unused(buf);

	create_table_main(self);

	//bench(self);
	
	int count = 10000000;
	uint64_t time = timer_mgr_gettime();

	uint64_t clients[10];
	for (int i = 0; i < 10; i++)
		clients[i] = coroutine_create(bench_client, self);

	for (int i = 0; i < 10; i++)
		coroutine_wait(clients[i]);

	time = (timer_mgr_gettime() - time) / 1000;

	// rps
	double rps = count / (float)(time / 1000.0 / 1000.0);
	portal_write(self->portal, make_float(rps));

#if 0
	// take shared catalog lock

	for (int i = 0; i < share->shard_mgr->shards_count; i++)
	{
		auto shard = share->shard_mgr->shards[i];
		auto req = request_create(&self->req_cache, &shard->task.channel);
		req->ro = false;
		request_set_add(&self->req_set, req);
	}

	// execute

	// todo: lock

	request_sched_lock(share->req_sched);
	request_set_execute(&self->req_set);
	request_sched_unlock(share->req_sched);

	// wait for completion
	request_set_wait(&self->req_set);

	// commit

	// todo: signal for completion

	// rw

	{
		list_foreach(&self->req_set.list)
		{
			auto req = list_at(Request, link);
			assert(req->on_commit);
			condition_signal(req->on_commit);
		}
	}
#endif
}

static inline void
session_prepare(Session* self)
{
	request_set_reset(&self->req_set, &self->req_cache);
}

hot bool
session_execute(Session* self, Buf* buf)
{
	Buf* reply;

	Exception e;
	if (try(&e))
	{
		auto msg = msg_of(buf);
		if (unlikely(msg->id != MSG_COMMAND))
			error("unrecognized request: %d", msg->id);

		session_prepare(self);

		execute(self, buf);
	}

	bool ro = false;
	if (catch(&e))
	{
		auto error = &mn_self()->error;
		reply = make_error(error);
		portal_write(self->portal, reply);
		ro = (error->code == ERROR_RO);
	}

	// ready
	reply = msg_create(MSG_OK);
	encode_integer(reply, false);
	msg_end(reply);
	portal_write(self->portal, reply);
	return ro;
}
