
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
#include <monotone_def.h>
#include <monotone_transaction.h>
#include <monotone_index.h>
#include <monotone_storage.h>
#include <monotone_wal.h>
#include <monotone_db.h>
#include <monotone_value.h>
#include <monotone_aggr.h>
#include <monotone_request.h>
#include <monotone_vm.h>
#include <monotone_parser.h>
#include <monotone_semantic.h>
#include <monotone_compiler.h>
#include <monotone_shard.h>
#include <monotone_hub.h>
#include <monotone_session.h>
#include <monotone_system.h>

static inline bool
system_is_client_only(void)
{
	return !var_string_is_set(&config()->directory);
}

static inline bool
system_is_backup(void)
{
	return var_string_is_set(&config()->backup);
}

static void
system_save_state(void *arg)
{
	System* self = arg;
	config_state_save(&self->config_state, global()->config);
}

static void
system_save_catalog(void *arg)
{
	System* self = arg;
	catalog_mgr_dump(&self->catalog_mgr, config_lsn());
}

System*
system_create(void)
{
	System* self = mn_malloc(sizeof(System));

	// set control
	auto control = &self->control;
	control->system       = &mn_task->channel;
	control->save_state   = system_save_state;
	control->save_catalog = system_save_catalog;
	control->arg          = self;
	global()->control     = control;

	// config state
	config_state_init(&self->config_state);
	catalog_mgr_init(&self->catalog_mgr);

	// server
	user_mgr_init(&self->user_mgr);
	server_init(&self->server);

	// cluster
	shard_mgr_init(&self->shard_mgr, &self->db, &self->function_mgr);
	hub_mgr_init(&self->hub_mgr);
	dispatch_lock_init(&self->dispatch_lock);
	router_init(&self->router);

	// db
	db_init(&self->db);

	// vm
	function_mgr_init(&self->function_mgr);

	// prepare shared context (shared between hubs)
	auto share = &self->share;
	share->function_mgr  = &self->function_mgr;
	share->schema_mgr    = &self->db.schema_mgr;
	share->view_mgr      = &self->db.view_mgr;
	share->table_mgr     = &self->db.table_mgr;
	share->wal           = &self->db.wal;
	share->db            = &self->db;
	share->shard_mgr     = &self->shard_mgr;
	share->dispatch_lock = &self->dispatch_lock;
	share->router        = &self->router;
	share->session_lock  = NULL;
	return self;
}

void
system_free(System* self)
{
	shard_mgr_free(&self->shard_mgr);
	router_free(&self->router);
	dispatch_lock_free(&self->dispatch_lock);
	server_free(&self->server);
	db_free(&self->db);
	function_mgr_free(&self->function_mgr);
	user_mgr_free(&self->user_mgr);
	config_state_free(&self->config_state);
	catalog_mgr_free(&self->catalog_mgr);
	mn_free(self);
}

static void
system_uuid_set(void)
{
	if (! var_string_is_set(&config()->uuid))
	{
		Uuid uuid;
		uuid_mgr_generate(global()->uuid_mgr, &uuid);
		char uuid_sz[UUID_SZ];
		uuid_to_string(&uuid, uuid_sz, sizeof(uuid_sz));
		var_string_set_raw(&config()->uuid, uuid_sz, sizeof(uuid_sz) - 1);
	}
}

static void
system_on_connect(Client* client, void* arg)
{
	auto buf = msg_create(MSG_CLIENT);
	buf_write(buf, &client, sizeof(void**));
	msg_end(buf);

	System* self = arg;
	hub_mgr_forward(&self->hub_mgr, buf);
}

static void*
system_session_create(Share* share, Portal* portal)
{
	return session_create(share, portal);
}

static void
system_session_free(void* session)
{
	session_free(session);
}

static void
system_session_execute(void* session, Buf* buf)
{
	session_execute(session, buf);
}

static HubIf hub_if =
{
	.session_create  = system_session_create,
	.session_free    = system_session_free,
	.session_execute = system_session_execute
};

void
system_start(System* self, bool bootstrap)
{
	// set uuid from config or generate a new one
	system_uuid_set();

	if (system_is_client_only())
	{
		// listen for relay connections only
		hub_mgr_start(&self->hub_mgr, &self->share, &hub_if, 1);
		return;
	}

	// hello
	log("");
	log("monotone.");
	log("");

	if (system_is_backup())
	{
		if (! bootstrap)
			error("restore: directory already exists");

		coroutine_set_name(mn_self(), "backup");
		// todo: restore

		// listen for relay connections only
		hub_mgr_start(&self->hub_mgr, &self->share, &hub_if, 1);
		return;
	}

	// create default config file
	char path[PATH_MAX];
	if (bootstrap)
	{
		snprintf(path, sizeof(path), "%s/monotone.conf", config_directory());
		config_create(config(), path);
	}

	// restore configuration state
	snprintf(path, sizeof(path), "%s/state", config_directory());
	config_state_open(&self->config_state, config(), path);

	// open user manager
	user_mgr_open(&self->user_mgr);

	// prepare builtin functions
	func_setup(&self->function_mgr);

	// prepare shards
	shard_mgr_open(&self->shard_mgr);
	if (! self->shard_mgr.shards_count)
	{
		auto shards = var_int_of(&config()->cluster_shards);
		shard_mgr_create(&self->shard_mgr, shards);
	}
	shard_mgr_set_partition_map(&self->shard_mgr, &self->router);

	// todo: prepare part mgr with shards

	// create system object and register catalogs
	db_open(&self->db, &self->catalog_mgr);

	// restore catalogs (partitions, schemas, tables, views)
	catalog_mgr_restore(&self->catalog_mgr);
	config_lsn_follow(var_int_of(&config()->catalog_snapshot));

	// start shards and recover partitions
	shard_mgr_start(&self->shard_mgr);

	// replay logs
	recover(&self->db);

	// todo: start checkpoint worker

	// start hubs
	auto hubs = var_int_of(&config()->cluster_hubs);
	hub_mgr_start(&self->hub_mgr, &self->share, &hub_if, hubs);

	// synchronize caches
	hub_mgr_sync_user_cache(&self->hub_mgr, &self->user_mgr.cache);

	log("");
	config_print(config());
	log("");

	// start server
	server_start(&self->server, system_on_connect, self);
}

void
system_stop(System* self)
{
	// stop server
	server_stop(&self->server);

	// stop hubs
	hub_mgr_stop(&self->hub_mgr);

	// stop shards
	shard_mgr_stop(&self->shard_mgr);

	// close db
	db_close(&self->db);

	// close config state file
	config_state_close(&self->config_state);
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
		system_ctl(self, session, stmt);
		break;
	}
	/*
	case RPC_USER_CREATE:
	{
		UserConfig* config = rpc_arg_ptr(rpc, 0);
		bool if_not_exists = rpc_arg(rpc, 1);
		user_mgr_create(&self->user_mgr, config, if_not_exists);
		hub_mgr_sync_user_cache(&self->hub_mgr, &self->user_mgr.cache);
		break;
	}
	case RPC_USER_DROP:
	{
		Str* name = rpc_arg_ptr(rpc, 0);
		bool if_exists = rpc_arg(rpc, 1);
		user_mgr_drop(&self->user_mgr, name, if_exists);
		hub_mgr_sync_user_cache(&self->hub_mgr, &self->user_mgr.cache);
		break;
	}
	case RPC_USER_ALTER:
	{
		UserConfig* config = rpc_arg_ptr(rpc, 0);
		user_mgr_alter(&self->user_mgr, config);
		hub_mgr_sync_user_cache(&self->hub_mgr, &self->user_mgr.cache);
		break;
	}
	*/
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
		auto buf = channel_read(&mn_task->channel, -1);
		auto msg = msg_of(buf);
		guard(buf_guard, buf_free, buf);

		// native connection
		if (msg->id == MSG_NATIVE)
		{
			unguard(&buf_guard);
			hub_mgr_forward(&self->hub_mgr, buf);
			continue;
		}

		if (msg->id == RPC_STOP)
			break;

		// system command
		rpc_execute(buf, system_rpc, self);
	}
}
