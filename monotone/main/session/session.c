
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
#include <monotone_engine.h>
#include <monotone_storage.h>
#include <monotone_wal.h>
#include <monotone_db.h>
#include <monotone_session.h>

#if 0
hot static inline void
commit(session *session)
{
	transaction *trx = &self->trx;
	if (unlikely(! transaction_active(trx)))
		error("%s", "transaction is not active");

	/* fast commit for queries and nested transactions */
	if (transaction_read_only(trx) || transaction_nested(trx))
	{
		mvcc_commit(trx);
		return;
	}

	/* check if system is in read-only mode */
	bool read_only;
	read_only = var_int_of(global()->config->read_only);
	if (unlikely(read_only))
	{
		mvcc_rollback(trx);
		error_as(ERROR_RO, "%s", "database is read-only, abort");
	}

	/* prepare transaction and write wal */

	/* resolve conflicts */
	bool conflict;
	conflict = mvcc_prepare(trx);
	if (conflict)
	{
		mvcc_rollback(trx);
		error_as(ERROR_CONFLICT, "%s", "transaction conflict detected");
	}

	/* wal write */
	wal_write(&self->db->wal, &trx->log);

	/* assign lsn */
	transaction_set_lsn(trx, trx->log.lsn);

	/* commit */
	mvcc_commit(trx);
}

hot static inline void
execute(session *session)
{
	transaction *trx = &self->trx;
	cmd *cmd = &self->query.cmd;

	/* start single-statement transaction only if the
	   command is transactional */
	if (! transaction_active(trx))
	{
		bool begin;
		begin = cmd->type->transactional;
		if (begin)
		{
			mvcc_begin(self->mvcc, trx);
			transaction_set_auto_commit(trx);
		}

	} else
	{
		/* prevent execution of non-transactional commands inside
		   multi-statement transaction */
		if (! cmd->type->transactional)
			cmd_error(cmd, "command is not transactional");
	}

	/* compile and execute command */
	try
	{
		query_execute(&self->query);
	} catch
	{
		if (trx->auto_commit && transaction_active(trx))
			mvcc_rollback(trx);
		rethrow();
	}

	/* auto-commit */
	if (trx->auto_commit && transaction_active(trx))
	{
		explat *explain;
		explain = &self->query.explain;
		explastart(explain, &explain->time_commit_us);
		session_commit(session);
		explaend(explain, &explain->time_commit_us);
	}

	/* EXPLAIN output */
	if (cmd->explain)
		query_explain(&self->query);
}
#endif

static void
benchmark_create_table(Session* self)
{
	auto trx = &self->trx;

	// create table
	mvcc_begin(trx);

	auto config = table_config_allocate();
	Str name;
	str_set_cstr(&name, "test");
	table_config_set_name(config, &name);

	config->config = storage_config_allocate();
	auto id = column_allocate();
	str_set_cstr(&name, "id");
	column_set_name(id, &name);
	column_set_type(id, TYPE_INT);
	schema_add_column(&config->config->schema, id);
	schema_add_key(&config->config->schema, id);

	table_mgr_create(&self->db->table_mgr, trx, config, true);
	table_config_free(config);

	wal_write(&self->db->wal, &trx->log);

	transaction_set_lsn(trx, trx->log.lsn);
	mvcc_commit(trx);
}

static void
benchmark_random(Session* self)
{
	auto trx = &self->trx;

	// create table
	benchmark_create_table(self);

	// insert
	Str name;
	str_set_cstr(&name, "test");
	auto table = table_mgr_find(&self->db->table_mgr, &name, true);

	uint64_t time = timer_mgr_gettime();

	int i = 0;
	int count = 10000000;
	for (; i < count; i++)
	{
		mvcc_begin(trx);

		auto buf = buf_create(0);
		encode_array(buf, 1);
		//encode_integer(buf, rand());
		encode_integer(buf, uuid_mgr_random(global()->uuid_mgr));

		storage_write(table->storage, trx, LOG_REPLACE, false, buf->start, buf_size(buf));
		buf_free(buf);

		mvcc_commit(trx);

		if ((i % 1000) == 0)
			coroutine_yield();
	}

	time = (timer_mgr_gettime() - time) / 1000;

	// time ms
	portal_write(self->portal, make_float((float)time / 1000.0));

	// rps
	double rps = count / (float)(time / 1000.0 / 1000.0);
	portal_write(self->portal, make_float(rps));

	// partitions
	portal_write(self->portal, make_integer( table->storage->engine.list_count ));

	engine_debug(&table->storage->engine);
}

static void
benchmark(Session* self)
{
	auto trx = &self->trx;

	// create table
	benchmark_create_table(self);

	// insert
	Str name;
	str_set_cstr(&name, "test");
	auto table = table_mgr_find(&self->db->table_mgr, &name, true);

	uint64_t time = timer_mgr_gettime();

	int i = 0;
	int count = 10000000;
	for (; i < count; i++)
	{
		mvcc_begin(trx);

		auto buf = buf_create(0);
		encode_array(buf, 1);
		encode_integer(buf, i);

		storage_write(table->storage, trx, LOG_REPLACE, false, buf->start, buf_size(buf));
		buf_free(buf);

		mvcc_commit(trx);

		if ((i % 10000) == 0)
			coroutine_yield();
	}

	time = (timer_mgr_gettime() - time) / 1000;

	// time ms
	portal_write(self->portal, make_float((float)time / 1000.0));

	// rps
	double rps = count / (float)(time / 1000.0 / 1000.0);
	portal_write(self->portal, make_float(rps));

	// partitions
	portal_write(self->portal, make_integer( table->storage->engine.list_count ));
}

static void
benchmark_wal_write_per_stmt(Session* self)
{
	auto trx = &self->trx;

	// create table
	benchmark_create_table(self);

	// insert
	Str name;
	str_set_cstr(&name, "test");
	auto table = table_mgr_find(&self->db->table_mgr, &name, true);

	uint64_t time = timer_mgr_gettime();

	int i = 0;
	int count = 1000000;
	for (; i < count; i++)
	{
		mvcc_begin(trx);

		auto buf = buf_create(0);
		encode_array(buf, 1);
		encode_integer(buf, i);

		storage_write(table->storage, trx, LOG_REPLACE, false, buf->start, buf_size(buf));
		buf_free(buf);

		//wal_write(&self->db->wal, &trx->log);
		wal_store_write(&self->db->wal.wal_store, &trx->log);
		transaction_set_lsn(trx, trx->log.lsn);

		mvcc_commit(trx);

		if ((i % 10000) == 0)
			coroutine_yield();
	}

	time = (timer_mgr_gettime() - time) / 1000;

	// time ms
	portal_write(self->portal, make_float((float)time / 1000.0));

	// rps
	double rps = count / (float)(time / 1000.0 / 1000.0);
	portal_write(self->portal, make_float(rps));

	// partitions
	portal_write(self->portal, make_integer( table->storage->engine.list_count ));
}

static void
benchmark_wal_write_per_batch(Session* self)
{
	auto trx = &self->trx;

	// create table
	benchmark_create_table(self);

	// insert
	Str name;
	str_set_cstr(&name, "test");
	auto table = table_mgr_find(&self->db->table_mgr, &name, true);

	uint64_t time = timer_mgr_gettime();

	int i = 0;
	int count = 10000000;
	int batch = 1000;
	int seq = 0;
	for (; i < count / batch; i++)
	{
		mvcc_begin(trx);

		for (int j = 0; j < batch; j++)
		{
			auto buf = buf_create(0);
			encode_array(buf, 1);
			encode_integer(buf, seq);

			storage_write(table->storage, trx, LOG_REPLACE, false, buf->start, buf_size(buf));
			buf_free(buf);

			seq++;
		}

		wal_write(&self->db->wal, &trx->log);
		//wal_store_write(&self->db->wal.wal_store, &trx->log);

		/*
		if ( ((i* batch) % 10000) == 0)
			coroutine_yield();
			*/

		transaction_set_lsn(trx, trx->log.lsn);
		mvcc_commit(trx);
	}

	time = (timer_mgr_gettime() - time) / 1000;

	// time ms
	portal_write(self->portal, make_float((float)time / 1000.0));

	// rps
	double rps = count / (float)(time / 1000.0 / 1000.0);
	portal_write(self->portal, make_float(rps));

	// partitions
	portal_write(self->portal, make_integer( table->storage->engine.list_count ));
}

static void
benchmark_client(void *arg)
{
	Session* self = arg;

	log("-- start");

	Transaction trx;
	transaction_init(&trx);

	// insert
	Str name;
	str_set_cstr(&name, "test");
	auto table = table_mgr_find(&self->db->table_mgr, &name, true);

	int i = 0;
	int count = 10000000 / 10;
	int batch = 1000;
	int seq = 0;
	for (; i < count / batch; i++)
	{
		mvcc_begin(&trx);

		for (int j = 0; j < batch; j++)
		{
			auto buf = buf_create(0);
			encode_array(buf, 1);
			encode_integer(buf, seq);

			storage_write(table->storage, &trx, LOG_REPLACE, false, buf->start, buf_size(buf));
			buf_free(buf);

			seq++;
		}

		wal_write(&self->db->wal, &trx.log);

		transaction_set_lsn(&trx, trx.log.lsn);
		mvcc_commit(&trx);
	}

	transaction_free(&trx);

	log("-- end");
}

static void
benchmark_10clients(Session* self)
{
	auto trx = &self->trx;

	// create table
	benchmark_create_table(self);

	uint64_t time = timer_mgr_gettime();

	int count = 10000000;

	uint64_t clients[10];
	for (int i = 0; i < 10; i++)
		clients[i] = coroutine_create(benchmark_client, self);

	for (int i = 0; i < 10; i++)
		coroutine_wait(clients[i]);

	time = (timer_mgr_gettime() - time) / 1000;

	// time ms
	portal_write(self->portal, make_float((float)time / 1000.0));

	// rps
	double rps = count / (float)(time / 1000.0 / 1000.0);
	portal_write(self->portal, make_float(rps));

}

Session*
session_create(Db*        db,
               ClientMgr* client_mgr,
               Portal*    portal)
{
	auto self = (Session*)mn_malloc(sizeof(Session));
	self->portal = portal;
	self->db     = db;
	transaction_init(&self->trx);
	(void)client_mgr;
	return self;
}

void
session_free(Session *self)
{
	auto trx = &self->trx;
	if (unlikely(transaction_active(trx)))
	{
		log("aborted active transaction");
		mvcc_abort(trx);
	}
	transaction_free(trx);
	mn_free(self);
}

hot bool
session_execute(Session* self, Buf* buf)
{
	auto trx = &self->trx;
	Buf* reply;

	Exception e;
	if (try(&e))
	{
		auto msg = msg_of(buf);
		if (unlikely(msg->id != MSG_QUERY))
			error("unrecognized request: %d", msg->id);

		// todo
		// portal_write(self->portal, config_list(config()));
		
		//benchmark(self);
		//benchmark_random(self);
		//benchmark_wal_write_per_stmt(self);
		//benchmark_wal_write_per_batch(self);
		benchmark_10clients(self);
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
	encode_integer(reply, transaction_active(trx));
	msg_end(reply);
	portal_write(self->portal, reply);
	return ro;
}
