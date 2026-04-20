
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

typedef struct Relay Relay;

struct Relay
{
	void*     session;
	Client*   client;
	bool      connected;
	Request   req;
	Frontend* fe;
};

static inline void
relay_init(Relay* self, Frontend* fe)
{
	self->session   = NULL;
	self->client    = NULL;
	self->connected = false;
	self->fe        = fe;
	request_init(&self->req);
}

static inline void
relay_free(Relay* self)
{
	if (self->client)
		client_free(self->client);
	if (self->session)
		self->fe->iface->session_free(self->session);
	request_free(&self->req);
}

static inline void
relay_connect(Relay* self)
{
	// create native session or remote connection
	auto endpoint = &self->req.endpoint;
	if (endpoint->proto.integer == PROTO_AMELIE)
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
		client_set_endpoint(self->client, endpoint);
		client_connect(self->client);
	}
	self->connected = true;
}

static inline void
relay_set(Relay* self, Buf* buf, Str* uri)
{
	request_reset(&self->req);

	// parse uri and configure endpoint
	auto endpoint = &self->req.endpoint;
	uri_parse(endpoint, uri);

	// set defaults
	if (opt_string_empty(&endpoint->user))
		str_set(&endpoint->user.string, "main", 4);

	if (opt_string_empty(&endpoint->content_type))
		opt_string_set_raw(&endpoint->content_type, "plain/text", 10);

	if (opt_string_empty(&endpoint->accept))
		opt_string_set_raw(&endpoint->accept, "application/json", 16);

	opt_int_set(&endpoint->endpoint, ENDPOINT_SQL);

	// authentication is not required
	opt_int_set(&endpoint->auth, false);

	// configure output
	auto output = &self->req.output;
	output_reset(output);
	output_set(output, endpoint);
	output_set_buf(output, buf);
}

hot static inline int
relay_execute_session(Relay* self, Str* command)
{
	auto code = 0;

	// authenticate
	auto req = &self->req;
	auto on_error = error_catch
	(
		request_auth(req, &self->fe->auth);
	);
	if (on_error)
	{
		// 403 Forbidden
		code = 403;
		goto done;
	}

	// set command
	req->type = REQUEST_SQL;
	req->text = *command;

	// execute
	auto ctl  = self->fe->iface;
	if (ctl->session_execute(self->session, req))
	{
		// 204 No Content
		// 200 OK
		if (buf_empty(req->output.buf))
			code = 204;
		else
			code = 200;
	} else
	{
		// 400 Bad Request
		code = 400;
	}

done:
	request_unlock(req);
	return code;
}

hot static inline int
relay_execute_client(Relay* self, Str* command)
{
	auto client = self->client;
	client_execute(client, command, self->req.output.buf);
	int64_t code = 0;
	str_toint(&client->reply.options[HTTP_CODE], &code);
	return code;
}

hot static inline int
relay_execute(Relay* self, Str* uri, NativeReq* cmd)
{
	relay_set(self, &cmd->output, uri);

	auto code = 0;
	auto on_error = error_catch
	(
		if (! self->connected)
			relay_connect(self);

		if (self->req.endpoint.proto.integer == PROTO_AMELIE)
			code = relay_execute_session(self, &cmd->cmd);
		else
			code = relay_execute_client(self, &cmd->cmd);
	);
	if (on_error)
	{
		buf_reset(&cmd->output);

		auto output = &self->req.output;
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
		auto req  = native_queue_pop(&native->queue);
		assert(req);
		switch (req->type) {
		case NATIVE_CONNECT:
			break;
		case NATIVE_DISCONNECT:
			connected = false;
			break;
		case NATIVE_EXECUTE:
			code = relay_execute(&relay, &native->uri, req);
			break;
		}
		native_req_complete(req, code);
	}
}
