
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
#include <sonata_http.h>
#include <sonata_client.h>
#include <sonata_server.h>
#include <sonata_row.h>
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
#include <sonata_cluster.h>
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
	cluster_init(&self->cluster, &self->db, &self->function_mgr);
	executor_init(&self->executor, &self->db, &self->cluster.router);

	// db
	db_init(&self->db, (PartMapper)cluster_map, &self->cluster);

	// replication
	repl_init(&self->repl, &self->db);

	// vm
	function_mgr_init(&self->function_mgr);

	// prepare shared context (shared between frontends)
	auto share = &self->share;
	share->executor     = &self->executor;
	share->repl         = &self->repl;
	share->cluster      = &self->cluster;
	share->frontend_mgr = &self->frontend_mgr;
	share->function_mgr = &self->function_mgr;
	share->user_mgr     = &self->user_mgr;
	share->catalog_mgr  = &self->catalog_mgr;
	share->db           = &self->db;
	return self;
}

void
system_free(System* self)
{
	repl_free(&self->repl);
	cluster_free(&self->cluster);
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

static void
system_recover(System* self)
{
	// ask each node to recover partitions in parallel
	Build build;
	build_init(&build, BUILD_RECOVER, &self->cluster, NULL, NULL);
	guard(build_free, &build);
	build_run(&build);

	// replay wals
	Recover recover;
	recover_init(&recover, &self->db, &build_if, &self->cluster);
	guard(recover_free, &recover);
	recover_wal(&recover);
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

	// prepare builtin functions
	func_setup(&self->function_mgr);

	// open user manager
	user_mgr_open(&self->user_mgr);

	// prepare cluster
	cluster_open(&self->cluster, bootstrap);

	// create system object and objects from last snapshot
	db_open(&self->db, &self->catalog_mgr);

	// start backend
	cluster_start(&self->cluster);

	// do parallel recover of snapshots and wal
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

	// prepare replication manager
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

	// stop cluster
	cluster_stop(&self->cluster);

	// close db
	db_close(&self->db);
}

static void
system_lock(System* self)
{
	frontend_mgr_lock(&self->frontend_mgr);
}

static void
system_unlock(System* self)
{
	frontend_mgr_unlock(&self->frontend_mgr);
}

static void
system_rpc(Rpc* rpc, void* arg)
{
	System* self = arg;
	switch (rpc->id) {
	case RPC_LOCK:
		system_lock(self);
		break;
	case RPC_UNLOCK:
		system_unlock(self);
		break;
	case RPC_SHOW_USERS:
	{
		Buf** buf = rpc_arg_ptr(rpc, 0);
		*buf = user_mgr_list(&self->user_mgr);
		break;
	}
	case RPC_SHOW_REPLICAS:
	{
		Buf** buf = rpc_arg_ptr(rpc, 0);
		*buf = replica_mgr_list(&self->repl.replica_mgr);
		break;
	}
	case RPC_SHOW_REPL:
	{
		Buf** buf = rpc_arg_ptr(rpc, 0);
		*buf = repl_show(&self->repl);
		break;
	}
	case RPC_SHOW_NODES:
	{
		Buf** buf = rpc_arg_ptr(rpc, 0);
		*buf = cluster_list(&self->cluster);
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
