
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
#include <monotone_session.h>
#include <monotone_core.h>

hot static void
core_native(Core* self, Native* client)
{
	unused(self);

	Portal portal;
	portal_init(&portal, portal_to_channel, &client->src);

	// create new session
	auto session = session_create(&portal);
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
		session_execute(session, buf);
	}
}

HubIf core_server_if =
{
	.main = 
	{
		[ACCESS_UNDEF]   = NULL,
		[ACCESS_CLIENT]  = NULL,
		[ACCESS_BACKUP]  = NULL,
		[ACCESS_REPLICA] = NULL,
	},
	.main_native = (HubMainNative)core_native
};
