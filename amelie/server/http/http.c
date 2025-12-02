
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
#include <amelie_http.h>

void
http_init(Http* self)
{
	self->headers_count = 0;
	for (int i = 0; i < HTTP_MAX; i++)
		str_init(&self->options[i]);
	buf_init(&self->headers);
	buf_init(&self->raw);
	buf_init(&self->content);
}

void
http_free(Http* self)
{
	buf_free(&self->headers);
	buf_free(&self->raw);
	buf_free(&self->content);
}

void
http_reset(Http* self)
{
	self->headers_count = 0;
	for (int i = 0; i < HTTP_MAX; i++)
		str_init(&self->options[i]);
	buf_reset(&self->headers);
	buf_reset(&self->raw);
	buf_reset(&self->content);
}

HttpHeader*
http_find(Http* self, char* name, int name_size)
{
	auto pos = (HttpHeader*)self->headers.start;
	auto end = (HttpHeader*)self->headers.position;
	for (; pos < end; pos++)
	{
		if (str_is_case(&pos->name, name, name_size))
			return pos;
	}
	return NULL;
}

void
http_log(Http* self)
{
	auto method  = &self->options[HTTP_METHOD];
	auto url     = &self->options[HTTP_URL];
	auto version = &self->options[HTTP_VERSION];
	auto code    = &self->options[HTTP_CODE];
	auto msg     = &self->options[HTTP_MSG];

	info("method:  %.*s", str_size(method), str_of(method));
	info("url:     %.*s", str_size(url), str_of(url));
	info("version: %.*s", str_size(version), str_of(version));
	info("code  :  %.*s", str_size(code), str_of(code));
	info("msg:     %.*s", str_size(msg), str_of(msg));

	auto header = (HttpHeader*)self->headers.start;
	auto end    = (HttpHeader*)self->headers.position;
	for (; header < end; header++)
	{
		info("%.*s: %.*s",
		     str_size(&header->name), str_of(&header->name),
		     str_size(&header->value), str_of(&header->value));
	}
}

hot static inline bool
http_read_header(Http* self, Readahead* readahead)
{
	for (;;)
	{
		uint8_t* pos = NULL;
		int size = readahead_read(readahead, 256, &pos);
		if (size == 0)
		{
			// eof
			if (unlikely(! buf_empty(&self->raw)))
				error("http: unexpected eof");
			return true;
		}
		buf_write(&self->raw, pos, size);

		// match header end marker
		auto start = self->raw.start;
		uint8_t* end = memmem(start, buf_size(&self->raw), "\r\n\r\n", 4);
		if (end)
		{
			int before_truncate = buf_size(&self->raw);
			self->raw.position = end + 4;

			int pushback = before_truncate - buf_size(&self->raw);
			readahead_pushback(readahead, pushback);
			break;
		}
		if (unlikely(buf_size(&self->raw) >= 512))
			error("HTTP header size limit exceeded");
	}
	return false;
}

hot bool
http_read(Http* self, Readahead* readahead, bool request)
{
	// read reader
	auto eof = http_read_header(self, readahead);
	if (unlikely(eof))
		return true;

	// parse header
	auto pos   = self->raw.start;
	auto start = pos;
	auto end   = pos + buf_size(&self->raw);

	// parse request or reply options
	Str *a, *b, *c;
	if (request)
	{
		// <method> <url> <version>\r\n
		a = &self->options[HTTP_METHOD];
		b = &self->options[HTTP_URL];
		c = &self->options[HTTP_VERSION];
	} else {
		// <version> <code> <msg>\r\n
		a = &self->options[HTTP_VERSION];
		b = &self->options[HTTP_CODE];
		c = &self->options[HTTP_MSG];
	}

	// a
	while (pos < end && *pos != ' ')
		pos++;
	if (unlikely(pos == end))
		goto error;
	str_set_u8(a, start, pos - start);
	pos++;
	if (unlikely(str_empty(a)))
		goto error;

	// b
	start = pos;
	while (pos < end && *pos != ' ')
		pos++;
	if (unlikely(pos == end))
		goto error;
	str_set_u8(b, start, pos - start);
	pos++;
	if (unlikely(str_empty(b)))
		goto error;

	// c
	start = pos;
	while (*pos != '\r')
		pos++;
	str_set_u8(c, start, pos - start);
	pos++;
	if (unlikely(*pos != '\n'))
		goto error;
	pos++;
	if (unlikely(str_empty(c)))
		goto error;

	// parse headers
	for (;;)
	{
		// \r\n (eof)
		if (unlikely(*pos == '\r'))
		{
			pos++;
			if (unlikely(*pos != '\n'))
				goto error;
			break;
		}

		auto header = (HttpHeader*)buf_emplace(&self->headers, sizeof(HttpHeader));
		self->headers_count++;

		// <name>: value\r\n
		start = pos;
		while (pos < end && *pos != ':')
			pos++;
		if (unlikely(pos == end))
			goto error;
		str_set_u8(&header->name, start, pos - start);
		pos++;

		// ' '
		if (unlikely(*pos != ' '))
			goto error;
		pos++;

		// value
		start = pos;
		while (*pos != '\r')
			pos++;
		str_set_u8(&header->value, start, pos - start);
		pos++;
		if (unlikely(*pos != '\n'))
			goto error;
		pos++;
	}

	return false;

error:
	error("failed to parse HTTP request");
}

hot bool
http_read_content_limit(Http* self, Readahead* readahead, Buf* content,
                        uint64_t limit)
{
	// read content
	auto content_len = http_find(self, "content-length", 14);
	if (! content_len)
		return false;

	int64_t len;
	if (unlikely(str_toint(&content_len->value, &len) == -1))
		error("failed to parse HTTP request");

	if (unlikely((uint64_t)len >= limit))
		return true;

	buf_reserve(content, len + 1);
	for (;;)
	{
		int left = len - buf_size(content);
		if (left == 0)
			break;
		int to_read = readahead->readahead;
		if (left < to_read)
			to_read = left;

		uint8_t* pos = NULL;
		int size = readahead_read(readahead, to_read, &pos);
		if (unlikely(size == 0))
			error("http: unexpected eof");
		buf_write(content, pos, size);
	}
	*content->position = '\0';
	return false;
}

hot void
http_read_content(Http* self, Readahead* readahead, Buf* content)
{
	http_read_content_limit(self, readahead, content, UINT64_MAX);
}

hot Buf*
http_begin_request(Http* self, Endpoint* endpoint, uint64_t size)
{
	// request
	auto buf = &self->raw;
	buf_reset(buf);

	// POST /v1/db/<db>
	// POST /v1/db/<db>/<relation>
	// POST /v1/<service>
	auto service  = opt_string_of(&endpoint->service);
	auto db       = opt_string_of(&endpoint->db);
	auto relation = opt_string_of(&endpoint->relation);

	buf_write(buf, "POST /", 6);
	if (! str_empty(service))
	{
		buf_write(buf, "v1/", 3);
		buf_write_str(buf, service);
	} else
	if (! str_empty(db))
	{
		buf_write(buf, "v1/db/", 6);
		buf_write_str(buf, db);
		if (! str_empty(relation))
		{
			buf_write(buf, "/", 1);
			buf_write_str(buf, relation);
		}
	}

	// arguments
	//
	// columns, timezone, format
	//
	bool first = true;
	uri_export_arg(&endpoint->columns, buf, &first);
	uri_export_arg(&endpoint->timezone, buf, &first);
	uri_export_arg(&endpoint->format, buf, &first);

	buf_write(buf, " HTTP/1.1\r\n", 11);

	// token
	auto token = opt_string_of(&endpoint->token);
	if (! str_empty(token))
	{
		buf_write(buf, "Authorization: Bearer ", 21);
		buf_write_str(buf, token);
		buf_write(buf, "\r\n", 2);
	}

	// content-type
	auto content_type = opt_string_of(&endpoint->content_type);
	if (! str_empty(content_type))
	{
		buf_write(buf, "Content-Type: ", 14);
		buf_write_str(buf, content_type);
		buf_write(buf, "\r\n", 2);
	}

	// content-length
	if (size > 0)
	{
		buf_write(buf, "Content-Length: ", 16);
		buf_printf(buf, "%" PRIu64, size);
		buf_write(buf, "\r\n", 2);
	}

	// accept
	auto accept = opt_string_of(&endpoint->accept);
	if (! str_empty(accept))
	{
		buf_write(buf, "Accept: ", 8);
		buf_write_str(buf, accept);
		buf_write(buf, "\r\n", 2);
	}

	return buf;
}

hot Buf*
http_begin_reply(Http*    self, Endpoint* endpoint,
                 char*    msg,
                 int      msg_size,
                 uint64_t size)
{
	// reply
	auto buf = &self->raw;
	buf_reset(buf);

	// HTTP/1.1 <code msg>
	buf_write(buf, "HTTP/1.1 ", 9);
	buf_write(buf, msg, msg_size);
	buf_write(buf, "\r\n", 2);

	// accept
	auto accept = opt_string_of(&endpoint->accept);
	if (! str_empty(accept))
	{
		buf_write(buf, "Content-Type: ", 14);
		buf_write_str(buf, accept);
		buf_write(buf, "\r\n", 2);
	}

	// content-length
	if (size > 0)
	{
		buf_write(buf, "Content-Length: ", 16);
		buf_printf(buf, "%" PRIu64, size);
		buf_write(buf, "\r\n", 2);
	}

	return buf;
}

void
http_end(Buf* buf)
{
	buf_write(buf, "\r\n", 2);
}
