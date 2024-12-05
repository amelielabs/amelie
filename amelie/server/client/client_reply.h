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
