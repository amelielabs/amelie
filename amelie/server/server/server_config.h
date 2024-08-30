#pragma once

//
// amelie.
//
// Real-Time SQL Database.
//

typedef struct ServerConfig ServerConfig;

struct ServerConfig
{
	Str              host;
	int64_t          host_port;
	struct addrinfo* host_addr;
	bool             tls;
	TlsContext       tls_context;
	Str              path;
	int              path_mode;
	Remote           remote;
	List             link;
};

static inline ServerConfig*
server_config_allocate(void)
{
	ServerConfig* self;
	self = am_malloc(sizeof(*self));
	self->host_port  = 3485;
	self->host_addr  = NULL;
	self->tls        = false;
	self->path_mode  = 0644;
	str_init(&self->host);
	str_init(&self->path);
	tls_context_init(&self->tls_context);
	remote_init(&self->remote);
	list_init(&self->link);
	return self;
}

static inline void
server_config_free(ServerConfig* self)
{
	if (self->host_addr)
		freeaddrinfo(self->host_addr);
	str_free(&self->path);
	str_free(&self->host);
	tls_context_free(&self->tls_context);
	remote_free(&self->remote);
	am_free(self);
}

static inline ServerConfig*
server_config_read(uint8_t** pos)
{
	auto self = server_config_allocate();
	guard(server_config_free, self);

	// map
	if (! data_is_map(*pos))
		error("server: listen[<{}>] element must be a map"); 
	data_read_map(pos);
	while (! data_read_map_end(pos))
	{
		// key
		Str name;
		data_read_string(pos, &name);

		// value
		if (str_compare_raw(&name, "path", 4))
		{
			// string
			if (! data_is_string(*pos))
				error("server: listen[] <path> must be a string");
			data_read_string_copy(pos, &self->path);
		} else
		if (str_compare_raw(&name, "path_mode", 9))
		{
			// string
			if (! data_is_string(*pos))
				error("server: listen[] <path_mode> must be a string");
			Str path_mode;
			data_read_string(pos, &path_mode);
			errno = 0;
			auto mode = strtol(str_of(&path_mode), NULL, 8);
			if (errno != 0)
				error("server: failed to read path_mode");
			self->path_mode = mode;
		} else
		if (str_compare_raw(&name, "host", 4))
		{
			// string
			if (! data_is_string(*pos))
				error("server: listen[] <host> must be a string");
			data_read_string_copy(pos, &self->host);
		} else
		if (str_compare_raw(&name, "port", 4))
		{
			// int
			if (! data_is_integer(*pos))
				error("server: listen[] <port> must be an integer");
			data_read_integer(pos, &self->host_port);
		} else
		if (str_compare_raw(&name, "tls", 3))
		{
			// bool
			if (! data_is_bool(*pos))
				error("server: listen[] <tls> must be a bool");
			data_read_bool(pos, &self->tls);
		} else
		if (str_compare_raw(&name, "tls_ca", 6))
		{
			// string
			if (! data_is_string(*pos))
				error("server: listen[] <tls_ca> must be a string");
			Str str;
			data_read_string(pos, &str);
			remote_set(&self->remote, REMOTE_FILE_CA, &str);
		} else
		if (str_compare_raw(&name, "tls_cert", 8))
		{
			// string
			if (! data_is_string(*pos))
				error("server: listen[] <tls_cert> must be a string");
			Str str;
			data_read_string(pos, &str);
			remote_set(&self->remote, REMOTE_FILE_CERT, &str);
		} else
		if (str_compare_raw(&name, "tls_key", 7))
		{
			// string
			if (! data_is_string(*pos))
				error("server: listen[] <tls_key> must be a string");
			Str str;
			data_read_string(pos, &str);
			remote_set(&self->remote, REMOTE_FILE_KEY, &str);
		} else
		{
			error("server: listen[] unknown listen option '%.*s'",
			      str_size(&name), str_of(&name));
		}
	}
	return unguard();
}
