
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
#include <sonata_node.h>
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

	// replication
	node_mgr_init(&self->node_mgr);
	repl_init(&self->repl, &self->db, &self->node_mgr);

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
	repl_free(&self->repl);
	node_mgr_free(&self->node_mgr);
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

static void
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

static void
system_configure(Str* options, bool bootstrap)
{
	auto config = config();

	char path[PATH_MAX];
	snprintf(path, sizeof(path), "%s/config.json", config_directory());
	if (bootstrap)
	{
		// set options first, to properly generate config
		if (! str_empty(options))
			config_set(config, options);

		// generate uuid, unless it is set
		if (! var_string_is_set(&config->uuid))
		{
			Uuid uuid;
			uuid_generate(&uuid, global()->random);
			char uuid_sz[UUID_SZ];
			uuid_to_string(&uuid, uuid_sz, sizeof(uuid_sz));
			var_string_set_raw(&config->uuid, uuid_sz, sizeof(uuid_sz) - 1);
		}

		// create config file
		config_open(config, path);
	} else
	{
		// open config file
		config_open(config, path);

		// redefine options
		if (! str_empty(options))
			config_set(config, options);
	}

	// reconfigure logger
	auto logger = global()->logger;
	logger_set_enable(logger, var_int_of(&config->log_enable));
	logger_set_to_stdout(logger, var_int_of(&config->log_to_stdout));
	if (! var_int_of(&config->log_to_file))
		logger_close(logger);
}

void
system_start(System* self, Str* options, bool bootstrap)
{
	// open or create config
	system_configure(options, bootstrap);

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
	shard_mgr_set_router(&self->shard_mgr, &self->router);

	// prepare executor
	executor_create(&self->executor);

	// create system object and objects from last snapshot
	db_open(&self->db, &self->catalog_mgr);

	// start shards
	shard_mgr_start(&self->shard_mgr);

	// recover
	system_recover(self);

	// set tables partition mapping
	list_foreach(&self->db.table_mgr.mgr.list)
	{
		auto table = table_of(list_at(Handle, link));
		shard_mgr_set_partition_map(&self->shard_mgr, &table->part_mgr);
	}

	// todo: start checkpoint worker

	// start frontends
	auto workers = var_int_of(&config()->frontends);
	frontend_mgr_start(&self->frontend_mgr,
	                   system_on_frontend_connect,
	                   self,
	                   workers);

	// synchronize caches
	frontend_mgr_sync(&self->frontend_mgr, &self->user_mgr.cache);

	// open node manager and prepare replication
	node_mgr_open(&self->node_mgr);
	repl_open(&self->repl);

	log("");
	config_print(config());
	log("");

	// start server
	server_start(&self->server, system_on_server_connect, self);

	// start replication
	if (var_int_of(&config()->repl))
	{
		var_int_set(&config()->repl, false);
		repl_start(&self->repl);
	}
}

void
system_stop(System* self)
{
	// stop server
	server_stop(&self->server);

	// stop replication
	repl_stop(&self->repl);

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
	case RPC_BACKUP:
	{
		frontend_mgr_lock(&self->frontend_mgr);
		guard(frontend_mgr_unlock, &self->frontend_mgr);

		Backup* backup = rpc_arg_ptr(rpc, 0);
		backup_prepare(backup);
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
