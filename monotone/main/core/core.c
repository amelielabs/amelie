
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
#include <monotone_db.h>
#include <monotone_shard.h>
#include <monotone_session.h>
#include <monotone_hub.h>
#include <monotone_core.h>

static inline bool
core_is_client_only(void)
{
	return !var_string_is_set(&config()->directory);
}

static inline bool
core_is_backup(void)
{
	return var_string_is_set(&config()->backup);
}

static void
core_save_config(void *arg)
{
	Core* self = arg;
	config_state_save(&self->config_state, global()->config);
}

static void
core_checkpoint(void* arg, bool full)
{
	(void)arg;
	(void)full;
	// todo
}

Core*
core_create(void)
{
	Core* self = mn_malloc(sizeof(Core));

	// set control
	auto control = &self->control;
	control->core        = &mn_task->channel;
	control->save_config = core_save_config;
	control->checkpoint  = core_checkpoint;
	control->arg         = self;
	global()->control    = control;

	// config state
	config_state_init(&self->config_state);
	catalog_mgr_init(&self->catalog_mgr);

	// server
	user_mgr_init(&self->user_mgr);
	server_init(&self->server);

	// cluster
	shard_mgr_init(&self->shard_mgr);
	hub_mgr_init(&self->hub_mgr);
	request_sched_init(&self->req_sched);

	// db
	db_init(&self->db, NULL, NULL);

	// shared between hubs
	auto share = &self->share;
	share->meta_mgr    = &self->db.meta_mgr;
	share->table_mgr   = &self->db.table_mgr;
	share->storage_mgr = &self->db.storage_mgr;
	share->shard_mgr   = &self->shard_mgr;
	share->req_sched   = &self->req_sched;
	share->cat_lock    = NULL;

	return self;
}

void
core_free(Core* self)
{
	shard_mgr_free(&self->shard_mgr);
	server_free(&self->server);
	request_sched_free(&self->req_sched);
	db_free(&self->db);
	user_mgr_free(&self->user_mgr);
	config_state_free(&self->config_state);
	catalog_mgr_free(&self->catalog_mgr);
	mn_free(self);
}

static void
core_uuid_set(void)
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
core_recover(Core* self)
{
	// restore tables and catalog objects
	catalog_mgr_restore(&self->catalog_mgr);
	config_lsn_follow(var_int_of(&config()->catalog_snapshot));

	// replay logs
	recover(&self->db);

	log("recover: complete");
}

static void
core_on_connect(Client* client, void* arg)
{
	auto buf = msg_create(MSG_CLIENT);
	buf_write(buf, &client, sizeof(void**));
	msg_end(buf);

	Core* self = arg;
	hub_mgr_forward(&self->hub_mgr, buf);
}

void
core_start(Core* self, bool bootstrap)
{
	// set uuid from config or generate a new one
	core_uuid_set();

	if (core_is_client_only())
	{
		// listen for relay connections only
		hub_mgr_start(&self->hub_mgr, &self->share, 1);
		return;
	}

	// hello
	log("");
	log("monotone.");
	log("");

	if (core_is_backup())
	{
		if (! bootstrap)
			error("restore: directory already exists");

		coroutine_set_name(mn_self(), "backup");
		// todo: restore

		// listen for relay connections only
		hub_mgr_start(&self->hub_mgr, &self->share, 1);
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

	// open db
	db_open(&self->db, &self->catalog_mgr);

	// prepare shards
	shard_mgr_open(&self->shard_mgr);
	if (! self->shard_mgr.shards_count)
	{
		auto shards = var_int_of(&config()->cluster_shards);
		shard_mgr_create(&self->shard_mgr, shards);
	}

	// set storages per shard
	shard_mgr_assign(&self->shard_mgr, &self->db.storage_mgr);

	// start shards and recover storages
	shard_mgr_start(&self->shard_mgr);

	// recover system to the previous state
	core_recover(self);

	// todo: start checkpoint worker

	// start hubs
	auto hubs = var_int_of(&config()->cluster_hubs);
	hub_mgr_start(&self->hub_mgr, &self->share, hubs);

	// synchronize caches
	hub_mgr_sync_user_cache(&self->hub_mgr, &self->user_mgr.cache);

	log("");
	config_print(config());
	log("");

	// start server
	server_start(&self->server, core_on_connect, self);
}

void
core_stop(Core* self)
{
	// stop server
	server_stop(&self->server);

	// stop hubs
	hub_mgr_stop(&self->hub_mgr);

	// stop shards
	shard_mgr_stop(&self->shard_mgr);

	// stop db
	db_close(&self->db);

	// close config state file
	config_state_close(&self->config_state);
}

static void
core_rpc(Rpc* rpc, void* arg)
{
	Core* self = arg;
	switch (rpc->id) {
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
		bool if_exists = rpc_arg(rpc, 2);
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

static void
core_catalog_lock(Core* self, RequestLock* req_lock)
{
	// get exlusive catalog lock on each hub
	hub_mgr_cat_lock(&self->hub_mgr);

	// notify on completion
	auto on_unlock = condition_create();
	req_lock->on_unlock = on_unlock;
	condition_signal(req_lock->on_lock);

	// wait for unlock
	condition_wait(on_unlock, -1);

	// release exclusive lock from hubs
	hub_mgr_cat_unlock(&self->hub_mgr);

	// done
	condition_free(on_unlock);
}

void
core_main(Core* self)
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

		if (msg->id == RPC_CAT_LOCK_REQ)
		{
			core_catalog_lock(self, request_lock_of(buf));
			continue;
		}

		// core command
		rpc_execute(buf, core_rpc, self);
	}
}
