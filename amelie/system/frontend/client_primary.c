
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
#include <amelie_sync>
#include <amelie_vm>
#include <amelie_frontend.h>

hot static void
frontend_client_primary_on_write(Node* self, NodeMsg* msg, Buf* data)
{
	auto fe = (Frontend*)((void**)self->execute_arg)[0];
	fe->iface->session_execute_msg(((void**)self->execute_arg)[1], self, msg, data);
}

void
frontend_client_primary(Frontend* self, Client* client, void* session)
{
	Recover recover;
	recover_init(&recover, share()->db, true);
	defer(recover_free, &recover);

	void* args[] = {self, session};
	Node node;
	node_init(&node, frontend_client_primary_on_write, args, &recover, client);
	node_main(&node);
}
