
//
// indigo
//	
// SQL OLTP database
//

#include <indigo_runtime.h>
#include <indigo_io.h>
#include <indigo_data.h>
#include <indigo_lib.h>
#include <indigo_config.h>
#include <indigo_auth.h>
#include <indigo_client.h>
#include <indigo_server.h>
#include <indigo_def.h>
#include <indigo_transaction.h>
#include <indigo_index.h>
#include <indigo_storage.h>
#include <indigo_wal.h>
#include <indigo_db.h>
#include <indigo_value.h>
#include <indigo_aggr.h>
#include <indigo_request.h>
#include <indigo_vm.h>
#include <indigo_parser.h>
#include <indigo_semantic.h>
#include <indigo_compiler.h>
#include <indigo_shard.h>
#include <indigo_hub.h>

hot void
hub_client(Hub* self, Client* client)
{
	unused(self);
	unused(client);
}

hot void
hub_native(Hub* self, Native* client)
{
	Portal portal;
	portal_init(&portal, portal_to_channel, &client->src);

	// create new session
	auto iface = self->iface;
	auto session = iface->session_create(&self->share, &portal);
	guard(on_close, self->iface->session_free, session);

	for (;;)
	{
		auto buf = channel_read(&client->system, -1);
		auto msg = msg_of(buf);
		guard(buf_guard, buf_free, buf);

		// connection closed
		if (msg->id == MSG_DISCONNECT)
			break;

		// execute command
		iface->session_execute(session, buf);
	}
}
