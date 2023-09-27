
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
}

void
session_free(Session *self)
{
	request_set_reset(&self->req_set);
	request_set_free(&self->req_set);
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
	transaction_begin(trx);

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
	transaction_commit(trx);

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

int seq = 0;

static void
bench_client(void* arg)
{
	Session* self = arg;
	auto share = self->share;

	RequestSet req_set;
	request_set_init(&req_set);
	request_set_create(&req_set, share->shard_mgr->shards_count);

	int count = 10000000 / 10;
	int batch = 1000;
	for (int i = 0; i < count / batch; i++)
	{
//		transaction_begin(trx);

		for (int j = 0; j < batch; j++)
		{
			//int    key = rand();
			int      key = seq++;
			uint8_t  key_data[16];
			uint8_t* key_data_pos = key_data;
			data_write_integer(&key_data_pos, key);

			// get shard
			auto shard = shard_map_get(share->shard_map, key_data, key_data_pos - key_data);

			// map request to the shard
			auto req = request_set_add(&req_set, shard->order, &shard->task.channel);

			encode_array(req->buf, 1);
			encode_integer(req->buf, key);
			req->buf_count++;
		}

		// execute
		request_sched_lock(share->req_sched);
		request_set_execute(&req_set);
		request_sched_unlock(share->req_sched);

		// wait for completion
		request_set_wait(&req_set);

		// todo: wal write
		uint64_t lsn = 0;

//		transaction_set_lsn(trx, lsn);
//		transaction_commit(trx);

		// signal shard on commit
		request_set_commit(&req_set, lsn);
		request_set_reset(&req_set);
	}

	request_set_free(&req_set);
}

hot static inline void
execute(Session* self, Buf* buf)
{
	unused(buf);
	create_table_main(self);

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
}

static inline void
session_prepare(Session* self)
{
	request_set_reset(&self->req_set);
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
