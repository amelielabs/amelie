
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
#include <monotone_value.h>
#include <monotone_aggr.h>
#include <monotone_vm.h>
#include <monotone_parser.h>
#include <monotone_compiler.h>
#include <monotone_shard.h>
#include <monotone_session.h>

void
session_init(Session* self, Share* share, Portal* portal)
{
	self->lock        = LOCK_NONE;
	self->lock_shared = NULL;
	self->share       = share;
	self->portal      = portal;
	request_lock_init(&self->lock_req);
	compiler_init(&self->compiler, share->db);
	command_init(&self->cmd);
	transaction_init(&self->trx);
	request_set_init(&self->req_set);
	log_set_init(&self->log_set);
}

void
session_free(Session *self)
{
	assert(self->lock == LOCK_NONE);
	compiler_free(&self->compiler);
	command_free(&self->cmd);
	request_set_reset(&self->req_set);
	request_set_free(&self->req_set);
	log_set_free(&self->log_set);
	transaction_free(&self->trx);
}

static void
session_lock(Session* self, SessionLock lock)
{
	assert(self->lock == LOCK_NONE);

	switch (lock) {
	case LOCK_SHARED:
		self->lock_shared = lock_lock(self->share->cat_lock, true);
		self->lock        = lock;
		break;
	case LOCK_EXCLUSIVE:
	{
		self->lock = lock;

		auto req = &self->lock_req;
		req->on_lock = condition_create();

		// RPC_CAT_LOCK_REQ
		channel_write(global()->control->core, request_lock_msg(req));

		// wait for exclusive lock to be taken on all hubs (including this one)
		condition_wait(req->on_lock, -1);
		break;
	}
	case LOCK_NONE:
		break;
	}
}

static void
session_unlock(Session* self)
{
	switch (self->lock) {
	case LOCK_SHARED:
	{
		lock_unlock(self->lock_shared);
		self->lock_shared = NULL;
		break;
	}
	case LOCK_EXCLUSIVE:
	{
		// ask core to unlock hubs
		auto req = &self->lock_req;
		if (req->on_lock)
		{
			condition_signal(req->on_unlock);
			condition_free(req->on_lock);
		}
		break;
	}
	case LOCK_NONE:
		break;
	}
	self->lock = LOCK_NONE;
}

#if 0
int seq = 0;

static void
bench_client(void* arg)
{
	Session* self = arg;
	auto share = self->share;

	// must take catalog lock

	RequestSet req_set;
	request_set_init(&req_set);

	LogSet log_set;
	log_set_init(&log_set);

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

		// add logs to the log set
		request_set_commit_prepare(&req_set, &log_set);

		// wal write
		wal_write(share->wal, &log_set);
		uint64_t lsn = log_set.lsn;

//		transaction_set_lsn(trx, lsn);
//		transaction_commit(trx);

		// signal shard on commit
		request_set_commit(&req_set, lsn);
		request_set_reset(&req_set);

		log_set_reset(&log_set);
	}

	request_set_free(&req_set);
	log_set_free(&log_set);
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
#endif

static inline void
execute_create_storages(Session* self, Table* table)
{
	auto share = self->share;

	// create storage for each shard
	for (int i = 0; i < share->shard_mgr->shards_count; i++)
	{
		auto shard = share->shard_mgr->shards[i];

		// create storage config
		auto config = storage_config_allocate();
		guard(config_guard, storage_config_free, config);

		Uuid id;
		uuid_mgr_generate(global()->uuid_mgr, &id);
		storage_config_set_id(config, &id);
		storage_config_set_id_table(config, &table->config->id);
		storage_config_set_id_shard(config, &shard->config->id);
		storage_config_set_range(config, shard->config->range_start,
		                         shard->config->range_end);

		// create storage
		auto storage = storage_mgr_create(share->storage_mgr, config);
		unguard(&config_guard);

		// create primary index per storage

		// create index config
		auto index_config = index_config_allocate();
		guard(index_config_guard, index_config_free, index_config);
		Str name;
		str_set_cstr(&name, "primary");
		index_config_set_name(index_config, &name);
		index_config_set_type(index_config, INDEX_TREE);
		index_config_set_primary(index_config, true);
		schema_copy(&index_config->schema, &table->config->schema);

		// create tree index
		auto index = tree_allocate(index_config, &storage->config->id);
		unguard(&index_config_guard);
		index_config_free(index_config);

		// attach index to the storage
		storage_mgr_attach(share->storage_mgr, storage, index);

		// attach and start storage on shard
		rpc(&shard->task.channel, RPC_STORAGE_ATTACH, 1, storage);
	}
}

static inline void
execute_create_table(Session* self, Ast* ast)
{
	auto trx = &self->trx;
	auto arg = ast_create_table_of(ast);
	auto share = self->share;

	// get exclusive catalog lock
	session_lock(self, LOCK_EXCLUSIVE);

	transaction_begin(trx);

	// create table
	table_mgr_create(share->table_mgr, trx, arg->config, arg->if_not_exists);
	if (transaction_read_only(trx))
	{
		// table exists
		transaction_commit(trx);
		return;
	}

	// wal write
	log_set_add(&self->log_set, &trx->log);
	wal_write(share->wal, &self->log_set);

	transaction_set_lsn(trx, trx->log.lsn);
	transaction_commit(trx);

	// create storage for each shard
	auto table = table_mgr_find(share->table_mgr, &arg->config->name, true);
	execute_create_storages(self, table);
}

static inline void
execute_drop_table(Session* self, Ast* ast)
{
	auto trx = &self->trx;
	auto arg = ast_drop_table_of(ast);
	auto share = self->share;

	// get exclusive catalog lock
	session_lock(self, LOCK_EXCLUSIVE);

	transaction_begin(trx);

	// drop table
	table_mgr_drop(share->table_mgr, trx, &arg->name->string, arg->if_exists);
	if (transaction_read_only(trx))
	{
		// table does not exists
		transaction_commit(trx);
		return;
	}

	// wal write
	log_set_add(&self->log_set, &trx->log);
	wal_write(share->wal, &self->log_set);

	transaction_set_lsn(trx, trx->log.lsn);
	transaction_commit(trx);
}

static inline Buf*
bench(Session* self)
{
	Compiler comp;
	compiler_init(&comp, self->share->db);

	int count = 10000000;
	uint64_t time = timer_mgr_gettime();

	int total = 0;

	Str query;
	str_set_cstr(&query,
	"REPLACE INTO test VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9),(10),(11),(12),(13),(14),(15),(16),(17),(18),(19),(20),(21),(22),(23),(24),(25),(26),(27),(28),(29),(30),(31),(32),(33),(34),(35),(36),(37),(38),(39),(40),(41),(42),(43),(44),(45),(46),(47),(48),(49),(50),(51),(52),(53),(54),(55),(56)");

	//lex_start(&comp.parser.lex, &query);

	for (int i = 0; i < count; i++)
	{
		palloc_truncate(0);

		/*
		int j = 0;
		while (j < 200)
		{
			ast(0);
			j++;
			total++;
		}
		*/

		/*
		bool eof = false;
		while (! eof)
		{
			auto ast = lex_next(&comp.parser.lex);
			eof = ast->id == 0;
			//parse(self->parser, ast->id, ast, &self->query);
		}
		*/

		compiler_reset(&comp);
		compiler_parse(&comp, &query);
		/*
		*/
	}

	time = (timer_mgr_gettime() - time) / 1000;

	// rps
	double rps = count / (float)(time / 1000.0 / 1000.0);
	//double rps = total / (float)(time / 1000.0 / 1000.0);
	return make_float(rps);
}

static inline void
execute_utility(Session* self)
{
	auto ast = compiler_first(&self->compiler);
	switch (ast->id) {
	case KSHOW:
	{
		// catalog lock
		session_lock(self, LOCK_SHARED);

		auto arg = ast_show_of(ast);
		Buf* buf = NULL;
		if (str_compare_raw(&arg->expr->string, "users", 5))
			rpc(global()->control->core, RPC_USER_SHOW, 1, &buf);
		else
		if (str_compare_raw(&arg->expr->string, "wal", 3))
			buf = wal_status(self->share->wal);
		else
		if (str_compare_raw(&arg->expr->string, "tables", 6))
			buf = table_mgr_list(&self->share->db->table_mgr);
		else
		if (str_compare_raw(&arg->expr->string, "views", 5))
			buf = meta_mgr_list(&self->share->db->meta_mgr);
		else
		if (str_compare_raw(&arg->expr->string, "all", 3))
			buf = config_list(global()->config);
		else
		if (str_compare_raw(&arg->expr->string, "be", 2))
		{
			buf = bench(self);
		}
		else
		{
			auto name = &arg->expr->string;
			auto var = config_find(global()->config, name);
			if (var && var_is(var, VAR_S))
				var = NULL;
			if (unlikely(var == NULL))
				error("SHOW name: '%.*s' not found", str_size(name),
				      str_of(name));
			buf = var_msg_create(var);
		}
		if (buf)
			portal_write(self->portal, buf);
		break;
	}
	case KSET:
	{
		auto arg = ast_set_of(ast);
		auto name = &arg->name->string;

		// find variable
		auto var = config_find(global()->config, name);
		if (var && var_is(var, VAR_S))
			var = NULL;
		if (unlikely(var == NULL))
			error("SET '%.*s': variable not found", str_size(name),
			      str_of(name));
		if (unlikely(! var_is(var, VAR_R)))
			error("SET '%.*s': variable is read-only", str_size(name),
			      str_of(name));

		// set value
		auto value = arg->value;
		switch (var->type) {
		case VAR_BOOL:
		{
			if (value->id != KTRUE && value->id != KFALSE)
				error("SET '%.*s': bool value expected", str_size(name),
				      str_of(name));
			bool is_true = value->id == KTRUE;
			var_int_set(var, is_true);
			break;
		}
		case VAR_INT:
		{
			if (value->id != KINT)
				error("SET '%.*s': integer value expected", str_size(name),
				      str_of(name));
			var_int_set(var, value->integer);
			break;
		}
		case VAR_STRING:
		{
			if (value->id != KSTRING)
				error("SET '%.*s': string value expected", str_size(name),
				      str_of(name));
			var_string_set(var, &value->string);
			break;
		}
		case VAR_DATA:
		{
			error("SET '%.*s': variable cannot be changed", str_size(name),
				  str_of(name));
			break;
		}
		}

		// save state for persistent vars
		if (var_is(var, VAR_P))
			control_save_config();
		break;
	}

	case KCREATE_USER:
	{
		auto arg = ast_create_user_of(ast);
		rpc(global()->control->core, RPC_USER_CREATE, 2,
		    arg->config, arg->if_not_exists);
		break;
	}

	case KDROP_USER:
	{
		auto arg = ast_drop_user_of(ast);
		rpc(global()->control->core, RPC_USER_DROP, 2,
		    &arg->name->string, arg->if_exists);
		break;
	}
	// todo: alter_user

	case KCREATE_TABLE:
		execute_create_table(self, ast);
		break;

	case KDROP_TABLE:
		execute_drop_table(self, ast);
		break;

	default:
		assert(0);
		break;
	}
}

static Code*
execute_add(uint32_t hash, void* arg)
{
	Session* self = arg;

	// get shard by hash
	auto shard = shard_map_get(self->share->shard_map, hash);

	// map request to the shard
	auto req = request_set_add(&self->req_set, shard->order, &shard->task.channel);
	return &req->code;
}

hot static inline void
execute_distributed(Session* self)
{
	auto share = self->share;
	auto req_set = &self->req_set;
	auto log_set = &self->log_set;

	// lock catalog
	session_lock(self, LOCK_SHARED);

	// generate request
	compiler_generate(&self->compiler, execute_add, self);
	request_set_finilize(req_set);

	// execute
	request_sched_lock(share->req_sched);
	request_set_execute(req_set);
	request_sched_unlock(share->req_sched);

	// wait for completion
	request_set_wait(req_set);

	// commit (no errors)
	if (likely(! req_set->error))
	{
		// todo: ro support
			// do nothing

		// add logs to the log set
		request_set_commit_prepare(req_set, log_set);

		// wal write
		//wal_write(share->wal, log_set);
		//uint64_t lsn = log_set->lsn;
		uint64_t lsn = 0;

		// signal shard on commit
		request_set_commit(req_set, lsn);
		return;
	}

	// abort transaction and throw error
	request_set_abort(req_set);
}

hot static inline void
execute(Session* self, Buf* buf)
{
	// read command
	command_set(&self->cmd, buf);

	// parse query
	compiler_parse(&self->compiler, &self->cmd.text);

	if (compiler_is_utility(&self->compiler))
	{
		// DDL or system command
		execute_utility(self);
	} else
	{
		// DML and Select
		execute_distributed(self);
	}

	session_unlock(self);

	palloc_truncate(0);
}

static inline void
session_reset(Session* self)
{
	auto share = self->share;
	palloc_truncate(0);
	if (likely(request_set_created(&self->req_set)))
		request_set_reset(&self->req_set);
	else
		request_set_create(&self->req_set,
		                   share->shard_mgr->shards_count);
	log_set_reset(&self->log_set);
	compiler_reset(&self->compiler);
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

		session_reset(self);
		execute(self, buf);
	}

	bool ro = false;
	if (catch(&e))
	{
		session_unlock(self);

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
