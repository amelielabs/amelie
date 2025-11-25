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

static inline void
client_send(Client* self, Str* command)
{
	// request
	auto request = &self->request;
	http_write_request(request, "POST /");
	auto token = remote_get(self->remote, REMOTE_TOKEN);
	if (! str_empty(token))
		http_write(request, "Authorization", "Bearer %.*s", str_size(token), str_of(token));
	http_write(request, "Content-Length", "%d", str_size(command));
	http_write(request, "Content-Type", "text/plain");
	http_write_end(request);
	tcp_write_pair_str(&self->tcp, &request->raw, command);
}

static inline void
client_send_import(Client* self, Str* path, Str* content_type, Str* content)
{
	// request
	auto request = &self->request;
	http_write_request(request, "POST %.*s", str_size(path), str_of(path));
	auto token = remote_get(self->remote, REMOTE_TOKEN);
	if (! str_empty(token))
		http_write(request, "Authorization", "Bearer %.*s", str_size(token), str_of(token));
	http_write(request, "Content-Type", "%.*s", str_size(content_type), str_of(content_type));
	http_write(request, "Content-Length", "%d", str_size(content));
	http_write_end(request);
	tcp_write_pair_str(&self->tcp, &request->raw, content);
}

static inline int
client_recv(Client* self, Buf* content)
{
	auto reply = &self->reply;
	http_reset(reply);
	auto eof = http_read(reply, &self->readahead, false);
	if (eof)
		error("unexpected eof");

	auto reply_into = content;
	if (! reply_into)
		reply_into = &reply->content;
	http_read_content(reply, &self->readahead, reply_into);

	int64_t code = 0;
	str_toint(&reply->options[HTTP_CODE], &code);
	return code;
}

static inline int
client_execute(Client* self, Str* command, Buf* content)
{
	client_send(self, command);
	return client_recv(self, content);
}

static inline int
client_execute_import(Client* self, Str* path, Str* content_type, Str* content)
{
	client_send_import(self, path, content_type, content);
	return client_recv(self, NULL);
}

static inline void
client_200(Client* self, Buf* content, char* mime)
{
	auto reply = &self->reply;
	http_write_reply(reply, 200, "OK");
	http_write(reply, "Content-Length", "%" PRIu64, buf_size(content));
	http_write(reply, "Content-Type", "%s", mime);
	http_write_end(reply);
	tcp_write_pair(&self->tcp, &reply->raw, content);
}

static inline void
client_204(Client* self)
{
	auto reply = &self->reply;
	http_write_reply(reply, 204, "No Content");
	http_write_end(reply);
	tcp_write_buf(&self->tcp, &reply->raw);
}

static inline void
client_400(Client* self, Buf* content, char* mime)
{
	auto reply = &self->reply;
	http_write_reply(reply, 400, "Bad Request");
	http_write(reply, "Content-Length", "%" PRIu64, buf_size(content));
	http_write(reply, "Content-Type", "%s", mime);
	http_write_end(reply);
	tcp_write_pair(&self->tcp, &reply->raw, content);
}

static inline void
client_403(Client* self)
{
	auto reply = &self->reply;
	http_write_reply(reply, 403, "Forbidden");
	http_write_end(reply);
	tcp_write_buf(&self->tcp, &reply->raw);
}

static inline void
client_413(Client* self)
{
	auto reply = &self->reply;
	http_write_reply(reply, 413, "Payload Too Large");
	http_write_end(reply);
	tcp_write_buf(&self->tcp, &reply->raw);
}
