
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

#include <amelie_runtime.h>
#include <amelie_io.h>
#include <amelie_lib.h>
#include <amelie_json.h>
#include <amelie_config.h>
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

		auto header = (HttpHeader*)buf_claim(&self->headers, sizeof(HttpHeader));
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

hot void
http_write_request(Http* self, char* fmt, ...)
{
	auto raw = &self->raw;
	buf_reset(raw);
	va_list args;
	va_start(args, fmt);
	buf_vprintf(raw, fmt, args);
	va_end(args);
	buf_write(raw, " HTTP/1.1\r\n", 11);
}

hot void
http_write_reply(Http* self, int code, char* msg)
{
	auto raw = &self->raw;
	buf_reset(raw);
	buf_printf(raw, "HTTP/1.1 %d %s\r\n", code, msg);
}

hot void
http_write(Http* self, char* name, char* fmt, ...)
{
	auto raw = &self->raw;
	buf_write(raw, name, strlen(name));
	buf_write(raw, ": ", 2);
	va_list args;
	va_start(args, fmt);
	buf_vprintf(raw, fmt, args);
	va_end(args);
	buf_write(raw, "\r\n", 2);
}

hot void
http_write_end(Http* self)
{
	buf_write(&self->raw, "\r\n", 2);
}
