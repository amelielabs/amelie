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

typedef struct ServerConfig ServerConfig;

struct ServerConfig
{
	Str              host;
	int64_t          port;
	bool             tls;
	bool             trusted;
	Str              tls_capath;
	Str              tls_ca;
	Str              tls_cert;
	Str              tls_key;
	Str              tls_server;
	TlsContext       tls_context;
	int              refs;
	struct addrinfo* ai;
};

static inline ServerConfig*
server_config_allocate(void)
{
	auto self = (ServerConfig*)am_malloc(sizeof(ServerConfig));
	self->port    = 8080;
	self->trusted = true;
	self->tls     = false;
	self->refs    = 0;
	self->ai      = NULL;
	str_init(&self->host);
	str_init(&self->tls_capath);
	str_init(&self->tls_ca);
	str_init(&self->tls_cert);
	str_init(&self->tls_key);
	str_init(&self->tls_server);
	tls_context_init(&self->tls_context);
	return self;
}

static inline void
server_config_free(ServerConfig* self)
{
	tls_context_free(&self->tls_context);
	str_free(&self->host);
	str_free(&self->tls_capath);
	str_free(&self->tls_ca);
	str_free(&self->tls_cert);
	str_free(&self->tls_key);
	str_free(&self->tls_server);
	if (self->ai)
		freeaddrinfo(self->ai);
	am_free(self);
}

static inline void
server_config_ref(ServerConfig* self)
{
	self->refs++;
}

static inline void
server_config_unref(ServerConfig* self)
{
	if (self->refs > 1)
	{
		self->refs--;
		return;
	}
	server_config_free(self);
}

static inline void
server_config_set_host(ServerConfig* self, Str* value)
{
	str_free(&self->host);
	str_copy(&self->host, value);
}

static inline void
server_config_set_port(ServerConfig* self, int value)
{
	self->port = value;
}

static inline void
server_config_set_tls(ServerConfig* self, bool value)
{
	self->tls = value;
}

static inline void
server_config_set_trusted(ServerConfig* self, bool value)
{
	self->trusted = value;
}

static inline void
server_config_set_tls_capath(ServerConfig* self, Str* value)
{
	str_free(&self->tls_capath);
	str_copy(&self->tls_capath, value);
}

static inline void
server_config_set_tls_ca(ServerConfig* self, Str* value)
{
	str_free(&self->tls_ca);
	str_copy(&self->tls_ca, value);
}

static inline void
server_config_set_tls_cert(ServerConfig* self, Str* value)
{
	str_free(&self->tls_cert);
	str_copy(&self->tls_cert, value);
}

static inline void
server_config_set_tls_key(ServerConfig* self, Str* value)
{
	str_free(&self->tls_key);
	str_copy(&self->tls_key, value);
}

static inline void
server_config_set_tls_server(ServerConfig* self, Str* value)
{
	str_free(&self->tls_server);
	str_copy(&self->tls_server, value);
}

static inline void
server_config_path(Str* self, Str* path)
{
	// relative to the cwd
	auto relative = str_is_prefix(path, "./", 2) ||
	                str_is_prefix(path, "../", 3);

	// absolute or relative file path
	if (*str_of(path) == '/' || relative)
	{
		str_copy(self, path);
		return;
	}

	// relative to the directory
	char relpath[PATH_MAX];
	auto relpath_size =
		format(relpath, sizeof(relpath), "{s}/{str}",
		       state_directory(), path);
	str_dup(self, relpath, relpath_size);
}

static inline void
server_config_tls(ServerConfig* self,
                  Str*          tls_capath,
                  Str*          tls_ca,
                  Str*          tls_cert,
                  Str*          tls_key)
{
	// tls_capath
	if (! str_empty(tls_capath))
		server_config_path(&self->tls_capath, tls_capath);

	// tls_ca
	if (! str_empty(tls_ca))
		server_config_path(&self->tls_ca, tls_ca);

	// tls_cert
	if (! str_empty(tls_cert))
		server_config_path(&self->tls_cert, tls_cert);

	// tls_key
	if (! str_empty(tls_key))
		server_config_path(&self->tls_key, tls_key);

	// create tls context
	if (self->tls && !str_empty(&self->tls_cert))
	{
		tls_context_set(&self->tls_context, false,
		                &self->tls_cert,
		                &self->tls_key,
		                &self->tls_ca,
		                &self->tls_capath,
		                &self->tls_server);
		tls_context_create(&self->tls_context);
	}
}

static inline ServerConfig*
server_config_read(uint8_t** pos)
{
	auto self = server_config_allocate();
	errdefer(server_config_free, self);
	Str tls_capath;
	Str tls_ca;
	Str tls_cert;
	Str tls_key;
	str_init(&tls_capath);
	str_init(&tls_ca);
	str_init(&tls_cert);
	str_init(&tls_key);
	Decode obj[] =
	{
		{ DECODE_STR,                 "host",       &self->host       },
		{ DECODE_INT|DECODE_OPT,      "port",       &self->port       },
		{ DECODE_BOOL|DECODE_OPT,     "trusted",    &self->trusted    },
		{ DECODE_BOOL|DECODE_OPT,     "tls",        &self->tls        },
		{ DECODE_STR_READ|DECODE_OPT, "tls_capath", &tls_capath       },
		{ DECODE_STR_READ|DECODE_OPT, "tls_ca",     &tls_ca           },
		{ DECODE_STR_READ|DECODE_OPT, "tls_cert",   &tls_cert         },
		{ DECODE_STR_READ|DECODE_OPT, "tls_key",    &tls_key          },
		{ DECODE_STR|DECODE_OPT,      "tls_server", &self->tls_server },
		{ 0,                           NULL,         NULL             },
	};
	decode_obj(obj, "server", pos);

	// configure and create server tls context
	server_config_tls(self, &tls_capath, &tls_ca, &tls_cert, &tls_key);
	return self;
}

static inline void
server_config_write(ServerConfig* self, Buf* buf, int flags)
{
	unused(flags);

	// obj
	encode_obj(buf);

	// host
	encode_raw(buf, "host", 4);
	encode_str(buf, &self->host);

	// port
	encode_raw(buf, "port", 4);
	encode_int(buf, self->port);

	// trusted
	encode_raw(buf, "trusted", 7);
	encode_bool(buf, self->trusted);

	// tls
	encode_raw(buf, "tls", 3);
	encode_bool(buf, self->tls);

	// tls_capath
	encode_raw(buf, "tls_capath", 10);
	encode_str(buf, &self->tls_capath);

	// tls_ca
	encode_raw(buf, "tls_ca", 6);
	encode_str(buf, &self->tls_ca);

	// tls_cert
	encode_raw(buf, "tls_cert", 8);
	encode_str(buf, &self->tls_cert);

	// tls_key
	encode_raw(buf, "tls_key", 7);
	encode_str(buf, &self->tls_key);

	// tls_server
	encode_raw(buf, "tls_server", 10);
	encode_str(buf, &self->tls_server);

	encode_obj_end(buf);
}
