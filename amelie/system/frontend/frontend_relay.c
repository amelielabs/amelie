
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
#include <amelie_db.h>
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
	Client* client;
	bool    connected;
	Remote  remote;
	Str     url;
	Str     content_type;
};

static inline void
relay_init(Relay* self)
{
	self->client = NULL;
	self->connected = false;
	remote_init(&self->remote);
	str_set_cstr(&self->url, "/");
	str_set_cstr(&self->content_type, "text/plain");
}

static inline void
relay_free(Relay* self)
{
	if (self->client)
		client_free(self->client);
	remote_free(&self->remote);
}

static inline void
relay_connect(Relay* self, Str* uri_str)
{
	// parse uri and prepare remote
	Uri uri;
	uri_init(&uri);
	defer(uri_free, &uri);
	uri_set(&uri, uri_str, false);
	uri_export(&uri, &self->remote);

	// create client
	if (self->client) {
		client_free(self->client);
		self->client = NULL;
	}
	self->client = client_create();
	client_set_remote(self->client, &self->remote);
	client_connect(self->client);
	self->connected = true;
}

hot static inline bool
relay_execute(Relay* self, Str* uri, Str* command, Content* output)
{
	content_reset(output);
	auto on_error = error_catch
	(
		if (! self->connected)
			relay_connect(self, uri);
		client_execute(self->client, command, output->content);
	);
	if (on_error) {
		self->connected = false;
		content_reset(output);
		content_write_json_error(output, &am_self()->error);
		return true;
	}

	// 200 OK
	// 204 No Content
	auto reply = &self->client->reply;
	if (str_is(&reply->options[HTTP_CODE], "200", 3) ||
	    str_is(&reply->options[HTTP_CODE], "204", 3))
		return false;

	// error content is already set

	// 400 Bad Request
	if (str_is(&reply->options[HTTP_CODE], "400", 3))
		return true;

	// 403 Forbidden
	// 413 Payload Too Large
	content_reset(output);
	content_write_json_error_as(output, &reply->options[HTTP_MSG]);
	return true;
}

void
frontend_relay(Frontend* self, Native* native)
{
	unused(self);

	Relay relay;
	relay_init(&relay);
	defer(relay_free, &relay);

	Content output;
	content_init(&output);
	for (auto connected = true; connected;)
	{
		auto req = request_queue_pop(&native->queue);
		assert(req);
		auto error = false;
		switch (req->type) {
		case REQUEST_CONNECT:
		{
			break;
		}
		case REQUEST_DISCONNECT:
		{
			connected = false;
			native_detach(native);
			break;
		}
		case REQUEST_EXECUTE:
		{
			content_set(&output, &req->content);
			error = relay_execute(&relay, &native->uri, &req->cmd, &output);
			break;
		}
		}
		request_complete(req, error);
	}
}
