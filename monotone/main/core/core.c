
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
	control->server      = &self->server.task.channel;
	control->save_config = core_save_config;
	control->checkpoint  = core_checkpoint;
	control->arg         = self;
	global()->control    = control;

	// config state
	config_state_init(&self->config_state);
	catalog_mgr_init(&self->catalog_mgr);

	// server
	client_mgr_init(&self->client_mgr);
	server_init(&self->server);

	// db
	db_init(&self->db, NULL, NULL);

	return self;
}

void
core_free(Core* self)
{
	db_free(&self->db);
	client_mgr_free(&self->client_mgr);
	server_free(&self->server);
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

	// replay wal
	recover(&self->db);

	log("recover: complete");
}

void
core_start(Core* self, bool bootstrap)
{
	// set uuid from config or generate a new one
	core_uuid_set();

	if (core_is_client_only())
	{
		// listen for relay connections only
		server_start(&self->server, false);
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
		server_start(&self->server, false);
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

	// open database
	db_open(&self->db, &self->catalog_mgr);

	// recover system to previous state
	core_recover(self);

	// todo: start checkpoint worker

	log("");
	config_print(config());
	log("");

	// start server
	server_start(&self->server, true);
}

void
core_stop(Core* self)
{
	// stop server
	server_stop(&self->server);

	// stop db
	db_close(&self->db);

	// close config state file
	config_state_close(&self->config_state);
}

static void
core_backup(Core* self, Client* client)
{
	(void)self;
	(void)client;
	// todo
}

hot static void
core_client_main(void* arg)
{
	Client* client = arg;
	Core* self = client->arg;

	// add client to the cores client manager
	client_mgr_add(&self->client_mgr, client);
	client_set_coroutine(client);
	client_set_coroutine_name(client);

	Exception e;
	if (try(&e))
	{
		// attach ipc to the task poller
		channel_attach(&client->core);

		// notify api
		client_on_connect(client);

		// process client
		switch (client->access) {
		case ACCESS_CLIENT:
			core_client(self, client);
			break;
		case ACCESS_BACKUP:
			core_backup(self, client);
			break;
		case ACCESS_REPLICA:
			// todo
			break;
		default:
			break;
		}
	}
	if (catch(&e))
	{ }

	// remove from the client mgr
	client_mgr_del(&self->client_mgr, client);

	// notify disconnect
	channel_detach(&client->core);
	client_on_disconnect(client);
}

static void
core_rpc(Rpc* rpc, void* arg)
{
	unused(rpc);
	unused(arg);
}

void
core_main(Core* self)
{
	for (;;)
	{
		auto buf = channel_read(&mn_task->channel, -1);
		auto msg = msg_of(buf);
		guard(buf_guard, buf_free, buf);

		// shutdown request
		if (unlikely(msg->id == RPC_STOP))
			break;

		// client connection
		if (msg->id == MSG_CLIENT)
		{
			Client* client = *(void**)msg->data;
			if (! str_empty(&client->uri))
			{
				// relay
				unguard(&buf_guard);
				channel_write(&self->server.task.channel, buf);
				continue;
			}
			// native
			client->arg = self;
			coroutine_create(core_client_main, client);
		} else
		{
			// core command
			rpc_execute(buf, core_rpc, self);
		}
	}
}
