
//
// amelie.
//
// Real-Time SQL OLTP Database.
//
// Copyright (c) 2024 Dmitry Simonenko.
// Copyright (c) 2024 Amelie Labs.
//
// AGPL-3.0 Licensed.
//

#include <amelie_runtime>
#include <amelie_server>
#include <amelie_db>
#include <amelie_repl.h>

void
node_init(Node*       self,
          NodeExecute execute,
          void*       execute_arg,
          Recover*    recover,
          Client*     client)
{
	self->client      = client;
	self->execute     = execute;
	self->execute_arg = execute_arg;
	self->recover     = recover;
	self->id_self     = *opt_uuid_of(&config()->uuid);
	uuid_init(&self->id_primary);

	// set websocket
	websocket_init(&self->websocket);
	Str protocol;
	str_set(&protocol, "amelie-repl", 11);
	websocket_set(&self->websocket, &protocol, client, false);
}

void
node_free(Node* self)
{
	websocket_free(&self->websocket);
}

void
node_main(Node* self)
{
	info("node connected.");

	// do websocket handshake
	auto websocket = &self->websocket;
	websocket_accept(websocket);

	auto buf = &self->client->request.content;
	for (;;)
	{
		// NODE_WRITE
		buf_reset(buf);

		NodeMsg msg;
		if (! node_recv(websocket, &msg, buf))
			break;
		if (msg.op != NODE_WRITE)
			error("node: unexpected message");

		// replay batch write
		if (! buf_empty(buf))
		{
			cancel_pause();
			self->execute(self, &msg, buf);
			cancel_resume();
		}

		// NODE_ACK
		node_send(websocket, NODE_ACK, &self->id_self,
		          state_lsn(), NULL);
	}
}
