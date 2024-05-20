
//
// sonata.
//
// SQL Database for JSON.
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
	str_init(&self->content);
	buf_init(&self->raw);
	buf_init(&self->headers);
	readahead_init(&self->readahead);
}

void
request_free(Request* self)
{
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
	str_init(&self->content);
	buf_reset(&self->raw);
	buf_reset(&self->headers);
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

	log("content: %.*s", str_size(&self->content), str_of(&self->content));
}

hot static inline void
request_process(Request* self, Tcp* tcp)
{
	// read reader
	Str header;
	readahead_header(&self->readahead, tcp, &header);

	// copy header
	buf_write_str(&self->raw, &header);	

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

	// validate header

	// read content
	auto content_len = request_find(self, "content-length", 14);
	if (content_len)
	{
		int64_t len;
		if (str_toint(&content_len->value, &len) == -1)
			error("failed to parse HTTP request");

		readahead_content(&self->readahead, tcp, &self->content, len);
	}
}
