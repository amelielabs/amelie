
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
#include <amelie_engine>
#include <amelie_storage>
#include <amelie_repl>
#include <amelie_vm>
#include <amelie_frontend.h>

typedef struct Relay Relay;

struct Relay
{
	void*     session;
	Client*   client;
	bool      connected;
	Output    output;
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
	output_init(&self->output);
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
	auto endpoint = &self->endpoint;
	endpoint_reset(endpoint);

	// parse uri and configure endpoint
	uri_parse(endpoint, uri);

	// set missing defaults
	if (opt_string_empty(&endpoint->db))
		opt_string_set_raw(&endpoint->db, "main", 4);

	if (opt_string_empty(&endpoint->content_type))
		opt_string_set_raw(&endpoint->content_type, "plain/text", 10);

	if (opt_string_empty(&endpoint->accept))
		opt_string_set_raw(&endpoint->accept, "application/json", 16);

	// configure output
	output_reset(&self->output);
	output_set(&self->output, &self->endpoint);

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
relay_execute_session(Relay* self, Str* command)
{
	auto ctl      = self->fe->iface;
	auto is_error = ctl->session_execute(self->session, &self->endpoint, command,
	                                     &self->output);
	auto code     = 0;
	if (is_error)
	{
		// 400 Bad Request
		code = 400;
	} else
	{
		// 204 No Content
		// 200 OK
		if (buf_empty(self->output.buf))
			code = 204;
		else
			code = 200;
	}
	return code;
}

hot static inline int
relay_execute_client(Relay* self, Str* command)
{
	auto client = self->client;
	client_execute(client, command, self->output.buf);
	int64_t code = 0;
	str_toint(&client->reply.options[HTTP_CODE], &code);
	return code;
}

hot static inline int
relay_execute(Relay* self, Str* uri, Request* req)
{
	auto output = &self->output;
	output_set_buf(output, &req->output);
	auto code = 0;
	auto on_error = error_catch
	(
		if (! self->connected)
			relay_connect(self, uri);
		if (self->endpoint.proto.integer == PROTO_AMELIE)
			code = relay_execute_session(self, &req->cmd);
		else
			code = relay_execute_client(self, &req->cmd);
	);
	if (on_error)
	{
		buf_reset(&req->output);
		if (output->iface)
			output_write_error(output, &am_self()->error);

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
	for (auto connected = true; connected;)
	{
		auto code = 0;
		auto req  = request_queue_pop(&native->queue);
		assert(req);
		switch (req->type) {
		case REQUEST_CONNECT:
			break;
		case REQUEST_DISCONNECT:
			connected = false;
			break;
		case REQUEST_EXECUTE:
			code = relay_execute(&relay, &native->uri, req);
			break;
		}
		request_complete(req, code);
	}
}
