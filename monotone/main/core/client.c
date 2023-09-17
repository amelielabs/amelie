
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

hot void
core_client(Core* self, Client* client)
{
	Portal portal;
	portal_init(&portal, portal_to_channel, &client->src);

	// create new session
	auto session = session_create(&self->db, &self->client_mgr, &portal);
	guard(on_close, session_free, session);

	for (;;)
	{
		auto buf = channel_read(&client->core, -1);
		auto msg = msg_of(buf);
		guard(buf_guard, buf_free, buf);

		// connection closed
		if (msg->id == MSG_DISCONNECT)
			break;

		// execute command
		bool ro = false;
		if (msg->id == MSG_QUERY)
			ro = session_execute(session, buf);

		// disconnect rw client on read-only error
		if (unlikely(ro && client->mode == ACCESS_MODE_RW))
			break;
	}
}
