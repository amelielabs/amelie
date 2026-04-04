
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
	uuid_init(&self->id_primary);
	uuid_init(&self->id_self);
	uuid_set(&self->id_self, &config()->uuid.string);
}

void
node_main(Node* self)
{
	info("node connected.");

	// do websocket handshake
	auto client = self->client;
	node_accept(client);

	auto buf = &client->request.content;
	for (;;)
	{
		// NODE_WRITE
		buf_reset(buf);
		WsFrame frame;
		NodeMsg msg;
		node_recv(client, &frame, &msg, buf);
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
		node_send(client, &frame, false, NODE_ACK, &self->id_self,
		          state_lsn(), NULL);
	}
}

void
node_validate(Node* self, NodeMsg* msg)
{
	// note: executed under shared catalog lock

	// check replication state
	if (unlikely(! opt_int_of(&state()->repl)))
		error("replication is disabled");

	if (uuid_empty(&self->id_primary))
	{
		auto primary = &state()->repl_primary.string;
		if (! str_empty(primary))
			uuid_set(&self->id_primary, primary);
	}

	if (uuid_compare(&msg->id, &self->id_primary))
		error("node: primary id mismatch");

	// lsn must much the current state
	if (msg->lsn != state_lsn())
		error("node: lsn does not match this server lsn");
}
