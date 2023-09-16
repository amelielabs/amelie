
//
// monotone
//	
// SQL OLTP database
//

#include <monotone_runtime.h>
#include <monotone_io.h>
#include <monotone_data.h>
#include <monotone_lib.h>

void
uri_init(Uri* self)
{
	self->proto       = URI_PROTO_DEFAULT;
	self->hosts_count = 0;
	self->args_count  = 0;
	self->pos         = NULL;
	str_init(&self->user);
	str_init(&self->password);
	str_init(&self->db);
	str_init(&self->uri);
	list_init(&self->hosts);
	list_init(&self->args);
}

void
uri_free(Uri* self)
{
	list_foreach_safe(&self->hosts)
	{
		auto host = list_at(UriHost, link);
		str_free(&host->host);
		mn_free(host);
	}

	list_foreach_safe(&self->args)
	{
		auto arg = list_at(UriArg, link);
		str_free(&arg->name);
		str_free(&arg->value);
		mn_free(arg);
	}

	str_free(&self->user);
	str_free(&self->password);
	str_free(&self->db);
	str_free(&self->uri);
}

static inline void
uri_error(void)
{
	error("failed to parse uri");
}

static inline void
uri_parse_protocol(Uri* self)
{
	if (! strncmp(self->pos, "indigo://", 9))
	{
		self->proto = URI_PROTO_DEFAULT;
		self->pos += 9;
	} else
	if (! strncmp(self->pos, "indigo+mock://", 14))
	{
		self->proto = URI_PROTO_MOCK;
		self->pos += 14;
	} else
	{
		// unsupported protocol
		char *protocol = strstr(self->pos, "://");
		if (protocol)
			uri_error();
	}
}

static inline void
uri_parse_user(Uri* self, bool safe)
{
	// [user[:password]@]
	char* at = strstr(self->pos, "@");
	if (! at)
		return;
	if (unlikely(at == self->pos))
		uri_error();

	char* sep = memchr(self->pos, ':', at - self->pos);
	if (sep)
	{
		// user:password
		str_strndup(&self->user, self->pos, sep - self->pos);

		sep++;
		if (safe)
		{
			str_strndup(&self->password, sep, at - sep);
		} else
		{
			// create password hash
			uint8_t digest[SHA1_DIGEST_LENGTH];
			char    digest_string[SHA1_DIGEST_STRING_LENGTH];
			Sha1 sha1;
			sha1_init(&sha1);
			sha1_add(&sha1, (uint8_t*)sep, at - sep);
			sha1_end(&sha1, digest);
			sha1_to_string(digest, digest_string, sizeof(digest_string));
			str_strndup(&self->password, digest_string, sizeof(digest_string) - 1);
		}
	} else
	{
		// user
		str_strndup(&self->user, self->pos, at - self->pos);
	}
	self->pos = at + 1;
}

static inline UriHost*
uri_add_host(Uri* self)
{
	auto host = (UriHost*)mn_malloc(sizeof(UriHost));
	memset(host, 0, sizeof(*host));
	host->port = 3485;
	str_init(&host->host);
	list_init(&host->link);
	list_append(&self->hosts, &host->link);
	self->hosts_count++;
	return host;
}

static inline void
uri_parse_host(Uri* self)
{
	// hostname[:port] [, ...] [/]
	for (;;)
	{
		auto host = uri_add_host(self);

		// hostname
		char *name = self->pos;
		int   name_size = 0;
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
		str_strndup(&host->host, name, name_size);

		// [:port]
		if (*self->pos == ':')
		{
			self->pos++;
			host->port = 0;
			char *start = self->pos;
			while (*self->pos && isdigit(*self->pos))
			{
				host->port = (host->port * 10) + (*self->pos - '0');
				self->pos++;
			}
			if (start == self->pos)
				uri_error();
		}

		// /
		if (*self->pos == '/') {
			self->pos++;
			break;
		}

		// ,
		if (*self->pos == ',') {
			self->pos++;
			continue;
		}

		// eof
		if (! *self->pos)
			return;

		uri_error();
	}
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
decode(Str* str, char* data, int data_size)
{
	char *start = mn_malloc(data_size + 1);
	char *pos = start;
	int i = 0;
	while (i < data_size)
	{
		char to_write;
		if (data[i] == '%') {
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
		*pos = to_write;
		pos++;
	}
	*pos = 0;
	str_set_allocated(str, start, pos - start);
}

static inline UriArg*
uri_add_arg(Uri* self)
{
	auto arg = (UriArg*)mn_malloc(sizeof(UriArg));
	str_init(&arg->name);
	str_init(&arg->value);
	list_init(&arg->link);
	list_append(&self->args, &arg->link);
	self->args_count++;
	return arg;
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

	for (;;)
	{
		// create new argument object
		auto arg = uri_add_arg(self);

		// name =
		int   name_size;
		char* name = self->pos;
		while (*self->pos && *self->pos != '=')
			self->pos++;
		if (*self->pos != '=')
			uri_error();
		name_size = self->pos - name;
		if (name_size == 0)
			uri_error();
		self->pos++;
		decode(&arg->name, name, name_size);

		// value [& ...]
		int   value_size;
		char* value = self->pos;
		while (*self->pos && *self->pos != '&')
			self->pos++;
		value_size = self->pos - value;
		if (value_size == 0)
			uri_error();
		decode(&arg->value, value, value_size);

		// eof
		if (! *self->pos)
			break;

		// &
		assert(*self->pos == '&');
		self->pos++;
	}
}

static inline void
uri_parse_db(Uri* self)
{
	int   name_size = 0;
	char* name = self->pos;
	while (*self->pos && *self->pos != '?')
		self->pos++;
	name_size = self->pos - name;
	if (name_size > 0)
		str_strndup(&self->db, name, name_size);
}

static inline void
uri_parse(Uri* self, bool safe, Str* spec)
{
	// set uri
	str_copy(&self->uri, spec);
	self->pos = self->uri.pos;

	// [indigo://]
	uri_parse_protocol(self);

	// [user[:password]@]
	uri_parse_user(self, safe);

	// hostname[:port] [, ...] [/]
	uri_parse_host(self);

	// [/db]
	uri_parse_db(self);

	// ?name=value[& ...]
	uri_parse_args(self);
}

void
uri_set(Uri* self, bool safe, Str* spec)
{
	// reset
	uri_free(self);
	uri_init(self);

	uri_parse(self, safe, spec);
}

UriArg*
uri_find(Uri* self, const char* name, int name_size)
{
	list_foreach(&self->args)
	{
		auto arg = list_at(UriArg, link);
		if (str_compare_raw(&arg->name, name, name_size))
			return arg;
	}
	return NULL;
}
