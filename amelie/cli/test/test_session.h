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
	Remote   remote;
	TestEnv* env;
	List     link;
};

static inline TestSession*
test_session_create(Str* name, TestEnv* env)
{
	TestSession* self = am_malloc(sizeof(*self));
	self->client = NULL;
	self->env    = env;
	remote_init(&self->remote);
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
	remote_free(&self->remote);
	str_free(&self->name);
	am_free(self);
}

static inline void
test_session_connect(TestSession* self, Str* uri, Str* cafile)
{
	// create client and connect
	remote_set(&self->remote, REMOTE_URI, uri);
	if (cafile && !str_empty(cafile))
		remote_set(&self->remote, REMOTE_FILE_CA, cafile);
	self->client = client_create();
	client_set_remote(self->client, &self->remote);
	client_connect(self->client);
}

static inline void
test_session_execute(TestSession* self,
                     Str*         path,
                     Str*         content_type,
                     Str*         content,
                     File*        output)
{
	auto client  = self->client;
	auto request = &client->request;
	auto reply   = &client->reply;
	auto token   = remote_get(client->remote, REMOTE_TOKEN);

	// request
	http_write_request(request, "POST %.*s", str_size(path), str_of(path));
	if (! str_empty(token))
		http_write(request, "Authorization", "Bearer %.*s", str_size(token), str_of(token));
	http_write(request, "Content-Type", "%.*s", str_size(content_type), str_of(content_type));
	http_write(request, "Content-Length", "%d", str_size(content));
	http_write_end(request);
	tcp_write_pair_str(&client->tcp, &request->raw, content);

	// reply
	http_reset(reply);
	auto eof = http_read(reply, &client->readahead, false);
	if (eof)
		error("unexpected eof");
	http_read_content(reply, &client->readahead, &reply->content);

	// 403 Forbidden
	if (str_is(&reply->options[HTTP_CODE], "403", 3))
	{
		auto msg = &reply->options[HTTP_MSG];
		file_write(output, str_of(msg), str_size(msg));
		file_write(output, "\n", 1);
		return;
	}

	// print
	if (! str_is(&reply->options[HTTP_CODE], "204", 3))
	{
		file_write(output, reply->content.start, buf_size(&reply->content));
		file_write(output, "\n", 1);
	}
}
