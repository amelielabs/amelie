
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

#if 0
hot static void
frontend_client_primary_on_write(Node* self, NodeMsg* msg, Buf* data)
{
	auto fe = (Frontend*)((void**)self->execute_arg)[0];
	fe->iface->session_execute_msg(((void**)self->execute_arg)[1], self, msg, data);
}
#endif

void
frontend_client_primary(Frontend* self, Client* client, void* session)
{
	(void)self;
	(void)client;
	(void)session;
#if 0
	Recover recover;
	recover_init(&recover, share()->db, true);
	defer(recover_free, &recover);

	void* args[] = {self, session};
	Node node;
	node_init(&node, frontend_client_primary_on_write, args, &recover, client);
	defer(node_free, &node);
	node_main(&node);
#endif
}
