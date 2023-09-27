
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
#include <monotone_shard.h>
#include <monotone_session.h>
#include <monotone_hub.h>

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

	// prepare new session
	Session session;
	session_init(&session, &self->share, &portal);
	guard(on_close, session_free, &session);

	for (;;)
	{
		auto buf = channel_read(&client->core, -1);
		auto msg = msg_of(buf);
		guard(buf_guard, buf_free, buf);

		// connection closed
		if (msg->id == MSG_DISCONNECT)
			break;

		// execute command
		session_execute(&session, buf);
	}
}
