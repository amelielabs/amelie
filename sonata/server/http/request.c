
//
// sonata.
//
// Real-Time SQL Database.
//

#include <sonata_runtime.h>
#include <sonata_io.h>
#include <sonata_lib.h>
#include <sonata_data.h>
#include <sonata_config.h>
#include <sonata_auth.h>
#include <sonata_http.h>

void
request_init(Request* self)
{
	self->headers_count = 0;
	str_init(&self->method);
	str_init(&self->url);
	str_init(&self->version);
	buf_init(&self->content);
	buf_init(&self->raw);
	buf_init(&self->headers);
	readahead_init(&self->readahead, 32 * 1024);
}

void
request_free(Request* self)
{
	buf_free(&self->content);
	buf_free(&self->raw);
	buf_free(&self->headers);
	readahead_free(&self->readahead);
}

void
request_reset(Request* self)
{
	self->headers_count = 0;
	str_init(&self->method);
	str_init(&self->url);
	str_init(&self->version);
	buf_reset(&self->raw);
	buf_reset(&self->headers);
	buf_reset(&self->content);
	readahead_reset(&self->readahead);
}

void
request_log(Request* self)
{
	log("method: %.*s", str_size(&self->method), str_of(&self->method));
	log("url: %.*s", str_size(&self->url), str_of(&self->url));
	log("version: %.*s", str_size(&self->version), str_of(&self->version));

	auto header = (RequestHeader*)self->headers.start;
	auto end    = (RequestHeader*)self->headers.position;
	for (; header < end; header++)
	{
		log("%.*s: %.*s",
		    str_size(&header->name), str_of(&header->name),
		    str_size(&header->value), str_of(&header->value));
	}

	log("content: %.*s", buf_size(&self->content), self->content.start);
}

hot static inline void
request_read_header(Request* self, Tcp* tcp)
{
	for (;;)
	{
		uint8_t* pos = NULL;
		int size = readahead_read(&self->readahead, tcp, 256, &pos);
		buf_write(&self->raw, pos, size);

		// match header end marker
		auto start = self->raw.start;
		uint8_t* end = memmem(start, buf_size(&self->raw), "\r\n\r\n", 4);
		if (end)
		{
			int before_truncate = buf_size(&self->raw);
			self->raw.position = end + 4;

			int pushback = before_truncate - buf_size(&self->raw);
			readahead_pushback(&self->readahead, pushback);
			break;
		}
		if (unlikely(buf_size(&self->raw) >= 512))
			error("HTTP header size limit exceeded");
	}
}

hot static inline void
request_process(Request* self, Tcp* tcp)
{
	// read reader
	request_read_header(self, tcp);

	// parse header
	auto pos = self->raw.start;
	auto end = pos + buf_size(&self->raw);

	// <method> <url> <version>\r\n

	// method
	auto start = pos;
	while (pos < end && *pos != ' ')
		pos++;
	if (unlikely(pos == end))
		goto error;
	str_set_u8(&self->method, start, pos - start);
	pos++;
	if (unlikely(str_empty(&self->method)))
		goto error;

	// url
	start = pos;
	while (pos < end && *pos != ' ')
		pos++;
	if (unlikely(pos == end))
		goto error;
	str_set_u8(&self->url, start, pos - start);
	pos++;
	if (unlikely(str_empty(&self->url)))
		goto error;

	// version
	start = pos;
	while (*pos != '\r')
		pos++;
	str_set_u8(&self->version, start, pos - start);
	pos++;
	if (unlikely(*pos != '\n'))
		goto error;
	pos++;
	if (unlikely(str_empty(&self->version)))
		goto error;

	// headers
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

		// reserve header
		RequestHeader* header;
		header = (RequestHeader*)buf_claim(&self->headers, sizeof(RequestHeader));
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
			error("");
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

	return;

error:
	error("failed to parse HTTP request");
}

static inline RequestHeader*
request_find(Request* self, const char* name, int name_size)
{
	auto pos = (RequestHeader*)self->headers.start;
	auto end = (RequestHeader*)self->headers.position;
	for (; pos < end; pos++)
	{
		if (str_strncasecmp(&pos->name, name, name_size))
			return pos;
	}
	return NULL;
}

hot void
request_read(Request* self, Tcp* tcp)
{
	// read header
	request_process(self, tcp);

	// todo: validate header

	// read content
	auto content_len = request_find(self, "content-length", 14);
	if (content_len)
	{
		int64_t len;
		if (str_toint(&content_len->value, &len) == -1)
			error("failed to parse HTTP request");

		buf_reserve(&self->content, len + 1);
		for (;;)
		{
			int left = len - buf_size(&self->content);
			if (left == 0)
				break;
			int to_read = self->readahead.readahead;
			if (left < to_read)
				to_read = left;

			uint8_t* pos = NULL;
			int size = readahead_read(&self->readahead, tcp, to_read, &pos);
			buf_write(&self->content, pos, size);
		}
		*self->content.position = '\0';
	}
}
