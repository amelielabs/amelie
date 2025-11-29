
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

#include <amelie_base.h>
#include <amelie_os.h>
#include <amelie_lib.h>
#include <amelie_json.h>
#include <amelie_runtime.h>
#include <amelie_user.h>
#include <amelie_auth.h>
#include <amelie_http.h>
#include <amelie_client.h>
#include <amelie_server.h>
#include <amelie_row.h>
#include <amelie_heap.h>
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_partition.h>
#include <amelie_checkpoint.h>
#include <amelie_catalog.h>
#include <amelie_wal.h>
#include <amelie_storage.h>
#include <amelie_backup.h>
#include <amelie_repl.h>
#include <amelie_value.h>
#include <amelie_set.h>
#include <amelie_content.h>
#include <amelie_executor.h>
#include <amelie_func.h>
#include <amelie_vm.h>
#include <amelie_frontend.h>

typedef struct Relay Relay;

struct Relay
{
	void*     session;
	Client*   client;
	bool      connected;
	Endpoint  endpoint;
	Frontend* fe;
};

static inline void
relay_init(Relay* self, Frontend* fe)
{
	self->session   = NULL;
	self->client    = NULL;
	self->connected = false;
	self->fe        = fe;
	endpoint_init(&self->endpoint);
}

static inline void
relay_free(Relay* self)
{
	if (self->client)
		client_free(self->client);
	if (self->session)
		self->fe->iface->session_free(self->session);
	endpoint_free(&self->endpoint);
}

static inline void
relay_connect(Relay* self, Str* uri)
{
	endpoint_reset(&self->endpoint);

	// parse uri
	uri_parse(&self->endpoint, uri);

	// create native session or remote connection
	if (self->endpoint.proto.integer == PROTO_AMELIE)
	{
		auto ctl = self->fe->iface;
		self->session = ctl->session_create(self->fe, self->fe->iface_arg);
	} else
	{
		if (self->client) {
			client_free(self->client);
			self->client = NULL;
		}
		self->client = client_create();
		client_set_endpoint(self->client, &self->endpoint);
		client_connect(self->client);
	}
	self->connected = true;
}

hot static inline int
relay_execute_session(Relay* self, Str* command, Content* output)
{
	auto ctl      = self->fe->iface;
	auto is_error = ctl->session_execute(self->session, &self->endpoint, command, output);
	auto code     = 0;
	if (is_error)
	{
		// 400 Bad Request
		code = 400;
	} else
	{
		// 204 No Content
		// 200 OK
		if (buf_empty(output->content))
			code = 204;
		else
			code = 200;
	}
	return code;
}

hot static inline int
relay_execute_client(Relay* self, Str* command, Content* output)
{
	auto client = self->client;
	client_execute(client, command, output->content);
	int64_t code = 0;
	str_toint(&client->reply.options[HTTP_CODE], &code);
	return code;
}

hot static inline int
relay_execute(Relay* self, Str* uri, Str* command, Content* output)
{
	content_reset(output);
	auto code = 0;
	auto on_error = error_catch
	(
		if (! self->connected)
			relay_connect(self, uri);
		if (self->endpoint.proto.integer == PROTO_AMELIE)
			code = relay_execute_session(self, command, output);
		else
			code = relay_execute_client(self, command, output);
	);
	if (on_error)
	{
		content_reset(output);
		content_write_json_error(output, &am_self()->error);

		// 502 Bad Gateway (connection or IO error)
		code = 502;
		self->connected = false;
	}
	return code;
}

void
frontend_native(Frontend* self, Native* native)
{
	Relay relay;
	relay_init(&relay, self);
	defer(relay_free, &relay);

	Content output;
	content_init(&output);
	for (auto connected = true; connected;)
	{
		auto req  = request_queue_pop(&native->queue);
		assert(req);
		auto code = 0;
		switch (req->type) {
		case REQUEST_CONNECT:
		{
			break;
		}
		case REQUEST_DISCONNECT:
		{
			connected = false;
			break;
		}
		case REQUEST_EXECUTE:
		{
			content_set(&output, &req->content);
			code = relay_execute(&relay, &native->uri, &req->cmd, &output);
			break;
		}
		}
		request_complete(req, code);
	}
}
