
//
// sonata.
//
// SQL Database for JSON.
//

#include <sonata_runtime.h>
#include <sonata_io.h>
#include <sonata_lib.h>
#include <sonata_data.h>
#include <sonata_config.h>
#include <sonata_auth.h>
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
#include <sonata_shard.h>
#include <sonata_frontend.h>
#include <sonata_session.h>
#include <sonata_main.h>

static void
system_save_config(void* arg)
{
	unused(arg);
	char path[PATH_MAX];
	snprintf(path, sizeof(path), "%s/config.json", config_directory());
	config_save(global()->config, path);
}

System*
system_create(void)
{
	System* self = so_malloc(sizeof(System));

	// set control
	auto control = &self->control;
	control->system      = &so_task->channel;
	control->save_config = system_save_config;
	control->arg         = self;
	global()->control    = control;

	// config state
	catalog_mgr_init(&self->catalog_mgr);

	// server
	user_mgr_init(&self->user_mgr);
	server_init(&self->server);

	// cluster
	frontend_mgr_init(&self->frontend_mgr);
	shard_mgr_init(&self->shard_mgr, &self->db, &self->function_mgr);
	router_init(&self->router);
	executor_init(&self->executor, &self->db, &self->router);

	// db
	db_init(&self->db);

	// vm
	function_mgr_init(&self->function_mgr);

	// prepare shared context (shared between frontends)
	auto share = &self->share;
	share->executor     = &self->executor;
	share->router       = &self->router;
	share->shard_mgr    = &self->shard_mgr;
	share->function_mgr = &self->function_mgr;
	share->db           = &self->db;
	return self;
}

void
system_free(System* self)
{
	shard_mgr_free(&self->shard_mgr);
	router_free(&self->router);
	executor_free(&self->executor);
	db_free(&self->db);
	function_mgr_free(&self->function_mgr);
	server_free(&self->server);
	user_mgr_free(&self->user_mgr);
	catalog_mgr_free(&self->catalog_mgr);
	so_free(self);
}

static void
system_on_server_connect(Server* server, Client* client)
{
	auto buf = msg_begin(MSG_CLIENT);
	buf_write(buf, &client, sizeof(void**));
	msg_end(buf);
	System* self = server->on_connect_arg;
	frontend_mgr_forward(&self->frontend_mgr, buf);
}

static void
system_on_frontend_connect(Frontend* frontend, Client* client)
{
	System* self = frontend->on_connect_arg;
	auto session = session_create(client, frontend, &self->share);
	guard(session_free, session);
	session_main(session);
}

void
system_recover(System* self)
{
	// do parallel recover per shard
	shard_mgr_recover(&self->shard_mgr);

	// wait for completion
	int complete = 0;
	int errors   = 0;
	while (complete < self->shard_mgr.shards_count)
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
	recover_wal(&self->db);
}

void
system_start(System* self, bool bootstrap)
{
	unused(bootstrap);

	// hello
	log("");
	log("sonata.");
	log("");

	// open user manager
	user_mgr_open(&self->user_mgr);

	// prepare builtin functions
	func_setup(&self->function_mgr);
	
	// prepare shards
	shard_mgr_open(&self->shard_mgr);
	if (! self->shard_mgr.shards_count)
	{
		auto shards = var_int_of(&config()->shards);
		shard_mgr_create(&self->shard_mgr, shards);
	}
	shard_mgr_set_partition_map(&self->shard_mgr, &self->router);

	// prepare executor
	executor_create(&self->executor);

	// create system object and objects from last snapshot
	db_open(&self->db, &self->catalog_mgr);

	// start shards
	shard_mgr_start(&self->shard_mgr);

	// recover
	system_recover(self);

	// todo: start checkpoint worker

	// start frontends
	auto workers = var_int_of(&config()->frontends);
	frontend_mgr_start(&self->frontend_mgr,
	                   system_on_frontend_connect,
	                   self,
	                   workers);

	// synchronize caches
	frontend_mgr_sync(&self->frontend_mgr, &self->user_mgr.cache);

	log("");
	config_print(config());
	log("");

	// start server
	server_start(&self->server, system_on_server_connect, self);
}

void
system_stop(System* self)
{
	// stop server
	server_stop(&self->server);

	// stop frontends
	frontend_mgr_stop(&self->frontend_mgr);

	// stop shards
	shard_mgr_stop(&self->shard_mgr);

	// close db
	db_close(&self->db);
}

static void
system_rpc(Rpc* rpc, void* arg)
{
	System* self = arg;
	switch (rpc->id) {
	case RPC_CTL:
	{
		Session* session = rpc_arg_ptr(rpc, 0);
		Stmt* stmt = rpc_arg_ptr(rpc, 1);
		Buf** buf = rpc_arg_ptr(rpc, 2);
		*buf = system_ctl(self, session, stmt);
		break;
	}
	case RPC_USER_SHOW:
	{
		Buf** buf = rpc_arg_ptr(rpc, 0);
		*buf = user_mgr_list(&self->user_mgr);
		break;
	}
	default:
		break;
	}
}

void
system_main(System* self)
{
	bool stop = false;
	while (! stop)
	{
		auto buf = channel_read(&so_task->channel, -1);
		auto msg = msg_of(buf);
		guard(buf_free, buf);

		if (msg->id == RPC_STOP)
			break;

		// system command
		rpc_execute(buf, system_rpc, self);
	}
}
