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
client_send(Client* self, Str* content)
{
	auto request = &self->request;
	auto buf = http_begin_request(request, HTTP_POST, self->endpoint, str_size(content));
	http_end(buf);
	tcp_write_pair_str(&self->tcp, buf, content);
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

	uint64_t code = 0;
	str_u64(&reply->options[HTTP_CODE], &code);
	return code;
}

static inline int
client_execute(Client* self, Str* content, Buf* result)
{
	auto histogram = self->histogram;
	uint64_t time_us;
	if (histogram)
		time_start(&time_us);

	client_send(self, content);
	auto code = client_recv(self, result);

	if (histogram)
	{
		time_end(&time_us);
		histogram_add(histogram, time_us / 1000);
	}
	return code;
}

static inline void
client_200(Client* self, Buf* content)
{
	auto reply = &self->reply;
	auto buf = http_begin_reply(reply, self->endpoint, "200 OK", 6, buf_size(content));
	http_end(buf);
	tcp_write_pair(&self->tcp, buf, content);
}

static inline void
client_204(Client* self)
{
	auto reply = &self->reply;
	auto buf = http_begin_reply(reply, self->endpoint, "204 No Content", 14, 0);
	http_end(buf);
	tcp_write_buf(&self->tcp, buf);
}

static inline void
client_400(Client* self, Buf* content)
{
	auto reply = &self->reply;
	auto buf = http_begin_reply(reply, self->endpoint, "400 Bad Request", 15,
	                            content? buf_size(content): 0);
	http_end(buf);
	if (content)
		tcp_write_pair(&self->tcp, buf, content);
	else
		tcp_write_buf(&self->tcp, buf);
}

static inline void
client_403(Client* self)
{
	auto reply = &self->reply;
	auto buf = http_begin_reply(reply, self->endpoint, "403 Forbidden", 13, 0);
	http_end(buf);
	tcp_write_buf(&self->tcp, buf);
}

static inline void
client_413(Client* self)
{
	auto reply = &self->reply;
	auto buf = http_begin_reply(reply, self->endpoint, "413 Payload Too Large", 21, 0);
	http_end(buf);
	tcp_write_buf(&self->tcp, buf);
}
