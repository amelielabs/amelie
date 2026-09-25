
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

#include <amelie_runtime>
#include <amelie_http.h>

typedef struct Uri Uri;

struct Uri
{
	char*     pos;
	Endpoint* endpoint;
};

static inline void
uri_error(void)
{
	error("failed to parse uri");
}

static inline void
uri_parse_protocol(Uri* self)
{
	if (! strncmp(self->pos, "http://", 7))
	{
		opt_int_set(&self->endpoint->proto, PROTO_HTTP);
		self->pos += 7;
	} else
	if (! strncmp(self->pos, "https://", 8))
	{
		opt_int_set(&self->endpoint->proto, PROTO_HTTPS);
		self->pos += 8;
	} else
	if (! strncmp(self->pos, "amelie://", 9))
	{
		opt_int_set(&self->endpoint->proto, PROTO_AMELIE);
		self->pos += 9;
	} else
	if (! strncmp(self->pos, "amelies://", 10))
	{
		opt_int_set(&self->endpoint->proto, PROTO_AMELIES);
		self->pos += 10;
	} else
	{
		// unsupported protocol
		char *protocol = strstr(self->pos, "://");
		if (protocol)
			uri_error();
	}
}

static inline void
uri_parse_user(Uri* self)
{
	// [user@]
	char* at = strstr(self->pos, "@");
	if (! at)
		return;
	if (unlikely(at == self->pos))
		uri_error();

	// ensure user:password@ is not supported
	auto sep = (char*)memchr(self->pos, ':', at - self->pos);
	if (sep)
		uri_error();

	// user
	auto endpoint = self->endpoint;
	opt_string_set_raw(&endpoint->user, self->pos, at - self->pos);
	self->pos = at + 1;
}

static inline void
uri_parse_host(Uri* self)
{
	// hostname[:port] [/ ?]

	// hostname
	auto name      = self->pos;
	int  name_size = 0;
	if (*name == '[')
	{
		// [name]
		name++;
		self->pos++;
		while (*self->pos && *self->pos != ']')
			self->pos++;
		if (unlikely(! *self->pos))
			uri_error();
		name_size = self->pos - name;
		self->pos++;

	} else
	{
		// name[:,/?]
		while (*self->pos &&
		       *self->pos != ':' &&
		       *self->pos != ',' &&
		       *self->pos != '/' &&
		       *self->pos != '?')
			self->pos++;
		name_size = self->pos - name;
	}
	if (name_size == 0)
		uri_error();

	// set host
	opt_string_set_raw(&self->endpoint->host, name, name_size);

	// [:port]
	if (*self->pos == ':')
	{
		self->pos++;
		int32_t port = 0;
		auto start = self->pos;
		while (*self->pos && isdigit(*self->pos))
		{
			if (unlikely(int32_mul_add_overflow(&port, port, 10, *self->pos - '0')))
				uri_error();
			self->pos++;
		}
		if (start == self->pos)
			uri_error();

		// set port
		opt_int_set(&self->endpoint->port, port);
	}

	// ?
	if (*self->pos == '?')
		return;

	// /
	if (*self->pos == '/')
		return;

	// eof
	if (! *self->pos)
		return;

	uri_error();
}

static inline int
decode_hex(char digit)
{
	int value = 0;
	if ('0' <= digit && digit <= '9')
		value = digit - '0';
	else if ('A' <= digit && digit <= 'F')
		value = digit - 'A' + 10;
	else if ('a' <= digit && digit <= 'f')
		value = digit - 'a' + 10;
	else
		error("failed to parse uri, incorrect percent value");
	return value;
}

hot static inline void
decode(Buf* buf, char* data, int data_size)
{
	int i = 0;
	while (i < data_size)
	{
		char to_write;
		if (data[i] == '%')
		{
			if ((data_size - i) < 3)
				error("failed to parse uri, incorrect percent value");
			int a = decode_hex(data[i + 1]);
			int b = decode_hex(data[i + 2]);
			to_write = (a << 4) | b;
			i += 3;
		} else {
			to_write = data[i];
			i++;
		}
		buf_write(buf, &to_write, 1);
	}
}

static inline void
uri_parse_args_set(Uri* self, int flags, Buf* buf, int name_size, int value_size)
{
	Str name;
	str_set(&name, buf_cstr(buf), name_size);
	Str value;
	str_set(&value, buf_cstr(buf) + name_size, value_size);

	// find and set endpoint option
	auto endpoint = self->endpoint;
	auto opt = opts_find(&endpoint->opts, &name);
	if (!opt || !opt_is(opt, OPT_C))
		error("unknown uri argument '{str}'", &name);

	// do not allow command line arguments
	if (flags && !opt_is(opt, flags))
		error("unknown uri argument '{str}'", &name);
	opt_set(opt, &value);
}

static inline void
uri_parse_args(Uri* self, int flags)
{
	// eof
	if (! *self->pos)
		return;

	// ?
	if (*self->pos != '?')
		uri_error();
	self->pos++;

	auto buf = buf_create();
	defer_buf(buf);
	for (;;)
	{
		buf_reset(buf);

		// name [= value] [&]
		int  name_size;
		auto name = self->pos;
		while (*self->pos && *self->pos != '=' && *self->pos != '&')
			self->pos++;
		name_size = self->pos - name;
		if (name_size == 0)
			uri_error();
		decode(buf, name, name_size);
		name_size = buf_size(buf);

		// value [&]
		int   value_size = 0;
		char* value = NULL;
		if (*self->pos == '=')
		{
			self->pos++;
			value = self->pos;
			while (*self->pos && *self->pos != '&')
				self->pos++;
			value_size = self->pos - value;
			if (value_size > 0)
			{
				decode(buf, value, value_size);
				value_size = buf_size(buf) - name_size;
			}
		}

		// match end set endpoint argument
		uri_parse_args_set(self, flags, buf, name_size, value_size);

		// eof
		if (! *self->pos)
			break;

		// &
		assert(*self->pos == '&');
		self->pos++;
	}
}

static inline void
uri_parse_endpoint(Uri* self)
{
	// /<endpoint> [?]
	auto start = self->pos;
	while (*self->pos && *self->pos != '?')
		self->pos++;

	opt_string_set_raw(&self->endpoint->endpoint, start, self->pos - start);
}

void
uri_parse(Endpoint* endpoint, Str* spec)
{
	// set uri
	opt_string_set(&endpoint->uri, spec);
	Uri self = {
		.pos      = opt_string_of(&endpoint->uri)->pos,
		.endpoint = endpoint
	};

	// [proto://]
	uri_parse_protocol(&self);

	// [user@]
	uri_parse_user(&self);

	// hostname[:port]
	uri_parse_host(&self);

	// [/endpoint]
	if (*self.pos == '/')
		uri_parse_endpoint(&self);

	// ?name=value[& ...]
	uri_parse_args(&self, OPT_A);
}

void
uri_parse_request(Endpoint* endpoint, Str* spec)
{
	if (str_empty(spec) || *spec->pos != '/')
		error("invalid endpoint");

	// set uri
	opt_string_set(&endpoint->uri, spec);
	Uri self = {
		.pos      = opt_string_of(&endpoint->uri)->pos,
		.endpoint = endpoint
	};

	// /<endpoint>
	uri_parse_endpoint(&self);

	// ?name=value[& ...]
	uri_parse_args(&self, OPT_AE);
}

void
uri_export_arg(Opt* opt, Buf* buf, bool* first)
{
	// [?|&]name=value
	switch (opt->type) {
	case OPT_BOOL:
		if (! opt_int_of(opt))
			return;
		buf_format(buf, "{c}{str}", *first? '?': '&',
		           &opt->name);
		break;
	case OPT_INT:
		buf_format(buf, "{c}{str}={u64}", *first? '?': '&',
		           &opt->name, opt->integer);
		break;
	case OPT_STRING:
		if (opt_string_empty(opt))
			return;
		buf_format(buf, "{c}{str}={str}", *first? '?': '&',
		           &opt->name, &opt->string);
		break;
	case OPT_JSON:
	case OPT_UUID:
		break;
	}
	*first = false;
}
