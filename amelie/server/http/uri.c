
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
	// [user[:password]@]
	char* at = strstr(self->pos, "@");
	if (! at)
		return;
	if (unlikely(at == self->pos))
		uri_error();

	auto endpoint = self->endpoint;
	auto sep = (char*)memchr(self->pos, ':', at - self->pos);
	if (sep)
	{
		// user:password
		opt_string_set_raw(&endpoint->user, self->pos, sep - self->pos);
		sep++;
		opt_string_set_raw(&endpoint->secret, sep, at - sep);
	} else
	{
		// user
		opt_string_set_raw(&endpoint->user, self->pos, at - self->pos);
	}
	self->pos = at + 1;
}

static inline void
uri_parse_host(Uri* self)
{
	// hostname[:port] [/]

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
		// name[:,/]
		while (*self->pos &&
		       *self->pos != ':' &&
		       *self->pos != ',' &&
		       *self->pos != '/')
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

	// /
	if (*self->pos == '/') {
		self->pos++;
		return;
	}

	// eof
	if (! *self->pos)
		return;

	uri_error();
}

static inline void
uri_parse_db(Uri* self)
{
	// /db
	int  name_size = 0;
	auto name = self->pos;
	while (*self->pos && *self->pos != '?')
		self->pos++;
	name_size = self->pos - name;
	if (name_size > 0)
		opt_string_set_raw(&self->endpoint->db, name, name_size);
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
uri_parse_args_set(Uri* self, Buf* buf, int name_size, int value_size)
{
	Str name;
	str_set(&name, buf_cstr(buf), name_size);
	Str value;
	str_set(&value, buf_cstr(buf) + name_size, value_size);

	// find and set endpoint option
	auto opt = opts_find(&self->endpoint->opts, &name);
	if (! opt)
		error("unknown uri argument '%.*s'", name_size, name);

	switch (opt->type) {
	case OPT_BOOL:
	{
		bool to = true;
		if (! str_empty(&value))
		{
			if (str_is_case(&value, "true", 4))
				to = true;
			else
			if (str_is_case(&value, "false", 5))
				to = false;
			else
				error("bool value expected for uri argument '%.*s'",
				      name_size, name);
		}
		opt_int_set(opt, to);
		break;
	}
	case OPT_INT:
	{
		int64_t to = 0;
		if (str_empty(&value) || str_toint(&value, &to) == -1)
			error("integer value expected for uri argument '%.*s'",
			      name_size, name);
		opt_int_set(opt, to);
		break;
	}
	case OPT_STRING:
		opt_string_set(opt, &value);
		break;
	default:
		abort();
	}
}

static inline void
uri_parse_args(Uri* self)
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

		// name =
		int  name_size;
		auto name = self->pos;
		while (*self->pos && *self->pos != '=')
			self->pos++;
		if (*self->pos != '=')
			uri_error();
		name_size = self->pos - name;
		if (name_size == 0)
			uri_error();
		self->pos++;
		decode(buf, name, name_size);
		name_size = buf_size(buf);

		// value [& ...]
		int   value_size;
		char* value = self->pos;
		while (*self->pos && *self->pos != '&')
			self->pos++;
		value_size = self->pos - value;
		if (value_size > 0)
		{
			decode(buf, value, value_size);
			value_size = buf_size(buf) - name_size;
		}

		// match end set endpoint argument
		uri_parse_args_set(self, buf, name_size, value_size);

		// eof
		if (! *self->pos)
			break;

		// &
		assert(*self->pos == '&');
		self->pos++;
	}
}

void
uri_parse(Endpoint* endpoint, Str* spec)
{
	// proto://[user:password@]host[:port]/db?arg=...&...

	// set uri
	opt_string_set(&endpoint->uri, spec);
	Uri self = {
		.pos      = opt_string_of(&endpoint->uri)->pos,
		.endpoint = endpoint
	};

	// [http://]
	uri_parse_protocol(&self);

	// [user[:password]@]
	uri_parse_user(&self);

	// hostname[:port] [, ...] [/]
	uri_parse_host(&self);

	// [/db]
	uri_parse_db(&self);

	// ?name=value[& ...]
	uri_parse_args(&self);
}

static inline void
uri_next(Uri* self, Str* value)
{
	// value[/]
	auto start = self->pos;
	while (*self->pos && *self->pos != '/')
		self->pos++;
	str_set(value, start, self->pos - start);
}

static inline void
uri_parse_endpoint_db(Uri* self)
{
	auto endpoint = self->endpoint;

	// db_name
	Str value;
	uri_next(self, &value);
	if (str_empty(&value))
		goto error;
	opt_string_set(&endpoint->db, &value);
	if (! *self->pos)
		return;
	// [/]
	self->pos++;

	// db_name/?
	if (!*self->pos || *self->pos == '?')
		return;

	// db_name/tables/name[/]
	// db_name/functions/name[/]
	uri_next(self, &value);
	if (str_empty(&value))
		goto error;
	auto is_table = false;
	if (str_is(&value, "tables", 6))
		is_table = true;
	else
	if (! str_is(&value, "functions", 9))
		is_table = false;
	else
		goto error;
	if (! *self->pos)
		return;
	self->pos++;

	// name[/]
	uri_next(self, &value);
	if (str_empty(&value))
		goto error;
	if (is_table)
		opt_string_set(&endpoint->table, &value);
	else
		opt_string_set(&endpoint->function, &value);
	if (! *self->pos)
		return;
	self->pos++;
	return;

error:
	error("failed to parse uri endpoint");
}

void
uri_parse_endpoint(Endpoint* endpoint, Str* spec)
{
	// /v1/db/<db_name>/tables/<name> [/?...]
	// /v1/db/<db_name>/functions/<name> [/?...]
	// /v1/db/<db_name> [/?...]
	// /v1/backup
	// /v1/repl

	// set uri
	opt_string_set(&endpoint->uri, spec);
	Uri self = {
		.pos      = opt_string_of(&endpoint->uri)->pos,
		.endpoint = endpoint
	};

	// /v1/db/
	if (likely(str_is_prefix(spec, "/v1/db/", 7)))
	{
		self.pos += 7;
		uri_parse_endpoint_db(&self);
	} else
	if (str_is(spec, "/v1/backup", 10))
	{
		str_set(&endpoint->service.string, "backup", 6);
	} else
	if (str_is(spec, "/v1/repl", 8))
	{
		str_set(&endpoint->service.string, "repl", 4);
	} else {
		error("failed to parse uri endpoint");
	}

	// ?name=value[& ...]
	uri_parse_args(&self);
}

void
uri_export(Endpoint* self, Buf* buf)
{
	// export endpoint as uri connection string

	// proto://
	if (self->proto.integer == PROTO_HTTP)
		buf_write(buf, "http://", 7);
	else
		buf_write(buf, "https://", 8);

	// [user[:password]@]
	if (! opt_string_is_set(&self->user))
	{
		buf_write_str(buf, &self->user.string);
		if (! opt_string_is_set(&self->secret))
		{
			buf_write(buf, ":", 1);
			buf_write_str(buf, &self->user.string);
		}
		buf_write(buf, "@", 1);
	}

	// hostname[:port]
	if (! opt_string_is_set(&self->host))
	{
		buf_write_str(buf, &self->host.string);
		buf_write(buf, ":", 1);
		buf_printf(buf, "%d", self->port.integer);
	}

	// /
	buf_write(buf, "/", 1);
	if (! opt_string_is_set(&self->db))
	{
		buf_write_str(buf, &self->db.string);
	}

	// ?name=value[& ...]
		// todo: ...
		// user, password, token
		// tls, table, function, timezone, ret, debug
}
