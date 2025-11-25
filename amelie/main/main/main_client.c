
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

#include <amelie_core.h>
#include <amelie.h>
#include <amelie_main.h>

typedef struct MainRemote MainRemote;

struct MainRemote
{
	MainClient obj;
	Client*    client;
};

static MainClient*
main_remote_create(Main* main)
{
	auto self = (MainRemote*)am_malloc(sizeof(MainRemote));
	self->obj.iface     = &main_remote;
	self->obj.pending   = 0;
	self->obj.histogram = NULL;
	self->obj.main      = main;
	list_init(&self->obj.link);
	self->client = client_create();
	return &self->obj;
}

static void
main_remote_free(MainClient* ptr)
{
	auto self = (MainRemote*)ptr;
	if (self->client)
	{
		client_close(self->client);
		client_free(self->client);
	}
	am_free(ptr);
}

static void
main_remote_connect(MainClient* ptr)
{
	auto self = (MainRemote*)ptr;
	client_set_remote(self->client, &self->obj.main->remote);
	client_connect(self->client);
}

hot static void
main_remote_send(MainClient* ptr, Str* command)
{
	auto self = (MainRemote*)ptr;
	client_send(self->client, command);
	self->obj.pending++;
}

hot static void
main_remote_send_import(MainClient* ptr, Str* path, Str* content_type, Str* content)
{
	auto self = (MainRemote*)ptr;
	client_send_import(self->client, path, content_type, content);
	self->obj.pending++;
}

hot static int
main_remote_recv(MainClient* ptr, Str* reply)
{
	auto self = (MainRemote*)ptr;
	auto code = client_recv(self->client, NULL);
	if (reply)
		buf_str(&self->client->reply.content, reply);
	self->obj.pending--;
	return code;
}

MainClientIf main_remote =
{
	.create      = main_remote_create,
	.free        = main_remote_free,
	.connect     = main_remote_connect,
	.send        = main_remote_send,
	.send_import = main_remote_send_import,
	.recv        = main_remote_recv
};

typedef struct MainLocal MainLocal;

struct MainLocal
{
	MainClient        obj;
	Event             event;
	amelie_session_t* session;
	amelie_request_t* req;
	Str               cmd;
};

static MainClient*
main_local_create(Main* main)
{
	auto self = (MainLocal*)am_malloc(sizeof(MainLocal));
	self->obj.iface     = &main_local;
	self->obj.pending   = 0;
	self->obj.histogram = NULL;
	self->obj.main      = main;
	list_init(&self->obj.link);
	self->session       = NULL;
	self->req           = NULL;
	event_init(&self->event);
	str_init(&self->cmd);
	return &self->obj;
}

static void
main_local_free(MainClient* ptr)
{
	auto self = (MainLocal*)ptr;
	if (self->req)
		amelie_free(self->req);
	amelie_free(self->session);
	str_free(&self->cmd);
	am_free(ptr);
}

static void
main_local_notify(void* arg)
{
	// wakeup on completion
	auto self = (MainLocal*)arg;
	event_signal(&self->event);
}

static void
main_local_connect(MainClient* ptr)
{
	auto self = (MainLocal*)ptr;
	self->session = amelie_connect(self->obj.main->env, NULL);
	if (unlikely(! self->session))
		error("amelie_connect() failed");
	event_attach(&self->event);
}

hot static void
main_local_send(MainClient* ptr, Str* cmd)
{
	// free previous request
	auto self = (MainLocal*)ptr;
	if (self->req)
	{
		amelie_free(self->req);
		self->req = NULL;
		str_free(&self->cmd);
	}

	str_copy(&self->cmd, cmd);
	self->req = amelie_execute(self->session, self->cmd.pos, 0, NULL,
	                           main_local_notify,
	                           self);
	self->obj.pending++;
}

hot static void
main_local_send_import(MainClient* ptr, Str* path, Str* content_type, Str* content)
{
	unused(ptr);
	unused(path);
	unused(content_type);
	unused(content);
	error("operation is not supported");
}

hot static int
main_local_recv(MainClient* ptr, Str* reply)
{
	auto self = (MainLocal*)ptr;
	event_wait(&self->event, UINT32_MAX);

	amelie_arg_t result = {NULL, 0};
	auto code = amelie_wait(self->req, -1, &result);
	if (reply)
		str_set(reply, result.data, result.data_size);

	self->obj.pending--;
	return code;
}

MainClientIf main_local =
{
	.create      = main_local_create,
	.free        = main_local_free,
	.connect     = main_local_connect,
	.send        = main_local_send,
	.send_import = main_local_send_import,
	.recv        = main_local_recv
};
