#pragma once

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

typedef struct TestSession TestSession;

struct TestSession
{
	Str      name;
	Client*  client;
	Endpoint endpoint;
	TestEnv* env;
	List     link;
};

static inline TestSession*
test_session_create(Str* name, TestEnv* env)
{
	TestSession* self = am_malloc(sizeof(*self));
	self->client = NULL;
	self->env    = env;
	endpoint_init(&self->endpoint);
	str_copy(&self->name, name);
	list_init(&self->link);
	return self;
}

static inline void
test_session_free(TestSession* self)
{
	list_unlink(&self->link);
	if (self->client)
	{
		client_close(self->client);
		client_free(self->client);
	}
	endpoint_free(&self->endpoint);
	str_free(&self->name);
	am_free(self);
}

static inline void
test_session_connect(TestSession* self, Str* uri, Str* cafile)
{
	// create client and connect
	auto endpoint = &self->endpoint;
	uri_parse(endpoint, uri);
	if (cafile && !str_empty(cafile))
		opt_string_set(&endpoint->tls_ca, cafile);

	// set defaults
	if (opt_string_empty(&endpoint->db))
		opt_string_set_raw(&endpoint->db, "main", 4);
	if (opt_string_empty(&endpoint->content_type))
		opt_string_set_raw(&endpoint->content_type, "plain/text", 10);
	if (opt_string_empty(&endpoint->accept))
		opt_string_set_raw(&endpoint->accept, "application/json", 16);

	self->client = client_create();
	client_set_endpoint(self->client, &self->endpoint);
	client_connect(self->client);
}

static inline void
test_session_execute(TestSession* self, Str* content, File* output)
{
	auto client = self->client;
	auto reply  = &client->reply;
	client_execute(client, content, &reply->content);

	if (buf_size(&reply->content))
	{
		file_write(output, reply->content.start, buf_size(&reply->content));
		file_write(output, "\n", 1);
	}

	int64_t code;
	str_toint(&reply->options[HTTP_CODE], &code);
	if (code == 200 || code == 204)
		return;

	if (buf_empty(&reply->content))
	{
		auto msg = &reply->options[HTTP_MSG];
		file_write(output, str_of(msg), str_size(msg));
		file_write(output, "\n", 1);
	}
}

static inline void
test_session_post(TestSession* self,
                  Str*         path,
                  Str*         content_type,
                  Str*         accept,
                  Str*         content,
                  File*        output)
{
	auto client  = self->client;
	auto request = &client->request;
	auto buf     = &request->raw;
	buf_reset(buf);

	// POST <path> HTTP/1.1
	buf_write(buf, "POST ", 5);
	buf_write_str(buf, path);
	buf_write(buf, " HTTP/1.1\r\n", 11);

	// content-type
	if (! str_empty(content_type))
	{
		buf_write(buf, "Content-Type: ", 14);
		buf_write_str(buf, content_type);
		buf_write(buf, "\r\n", 2);
	}

	// content-length
	if (! str_empty(content))
	{
		buf_write(buf, "Content-Length: ", 16);
		buf_printf(buf, "%d", str_size(content));
		buf_write(buf, "\r\n", 2);
	}

	// accept
	if (! str_empty(accept))
	{
		buf_write(buf, "Accept: ", 8);
		buf_write_str(buf, accept);
		buf_write(buf, "\r\n", 2);
	}
	buf_write(buf, "\r\n", 2);

	// send
	if (! str_empty(content))
		tcp_write_pair_str(&client->tcp, buf, content);
	else
		tcp_write_buf(&client->tcp, buf);

	// recv
	auto reply  = &client->reply;
	client_recv(client, &reply->content);
	if (buf_size(&reply->content))
	{
		file_write(output, reply->content.start, buf_size(&reply->content));
		file_write(output, "\n", 1);
	}

	int64_t code;
	str_toint(&reply->options[HTTP_CODE], &code);
	if (code == 200 || code == 204)
		return;

	if (buf_empty(&reply->content))
	{
		auto code = &reply->options[HTTP_CODE];
		auto msg  = &reply->options[HTTP_MSG];
		char text[64];
		auto text_size =
			snprintf(text, sizeof(text), "%.*s %.*s\n", str_size(code), str_of(code),
			         str_size(msg), str_of(msg));
		file_write(output, text, text_size);
	}
}
