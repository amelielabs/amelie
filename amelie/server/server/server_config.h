#pragma once

//
// amelie.
//
// Real-Time SQL Database.
//

typedef struct ServerConfig ServerConfig;

struct ServerConfig
{
	bool             tls;
	TlsContext       tls_context;
	Remote           remote;
	Str              path;
	Str              path_mode;
	Str              host;
	struct addrinfo* host_addr;
	int64_t          port;
	List             link;
};

static inline ServerConfig*
server_config_allocate(void)
{
	ServerConfig* self;
	self = am_malloc(sizeof(*self));
	self->tls        = false;
	self->host_addr  = NULL;
	self->port       = 3485;
	str_init(&self->path);
	str_init(&self->path_mode);
	str_init(&self->host);
	tls_context_init(&self->tls_context);
	remote_init(&self->remote);
	list_init(&self->link);
	return self;
}

static inline void
server_config_free(ServerConfig* self)
{
	tls_context_free(&self->tls_context);
	remote_free(&self->remote);
	if (self->host_addr)
		freeaddrinfo(self->host_addr);
	str_free(&self->path);
	str_free(&self->path_mode);
	str_free(&self->host);
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
		if (str_compare_raw(&name, "tls", 3))
		{
			// bool
			data_read_bool(pos, &self->tls);
		} else
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
			data_read_string_copy(pos, &self->path_mode);
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
			data_read_integer(pos, &self->port);
		} else
		{
			error("server: listen[] unknown listen option '%.*s'",
			      str_size(&name), str_of(&name)); 
		}
	}
	return unguard();
}
