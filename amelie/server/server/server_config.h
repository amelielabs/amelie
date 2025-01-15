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
	int64_t          host_port;
	struct addrinfo* host_addr;
	bool             auth;
	bool             tls;
	Str              path;
	int              path_mode;
	List             link;
};

static inline ServerConfig*
server_config_allocate(void)
{
	ServerConfig* self;
	self = am_malloc(sizeof(*self));
	self->host_port  = 3485;
	self->host_addr  = NULL;
	self->auth       = false;
	self->tls        = false;
	self->path_mode  = 0644;
	str_init(&self->host);
	str_init(&self->path);
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
	am_free(self);
}

static inline ServerConfig*
server_config_read(uint8_t** pos)
{
	auto self = server_config_allocate();
	errdefer(server_config_free, self);

	// obj
	if (! json_is_obj(*pos))
		error("server: listen[<{}>] element must be an object");
	json_read_obj(pos);
	while (! json_read_obj_end(pos))
	{
		// key
		Str name;
		json_read_string(pos, &name);

		// value
		if (str_is(&name, "path", 4))
		{
			// string
			if (! json_is_string(*pos))
				error("server: listen[] <path> must be a string");
			json_read_string_copy(pos, &self->path);
		} else
		if (str_is(&name, "path_mode", 9))
		{
			// string
			if (! json_is_string(*pos))
				error("server: listen[] <path_mode> must be a string");
			Str path_mode;
			json_read_string(pos, &path_mode);
			errno = 0;
			auto mode = strtol(str_of(&path_mode), NULL, 8);
			if (errno != 0)
				error("server: failed to read path_mode");
			self->path_mode = mode;
		} else
		if (str_is(&name, "host", 4))
		{
			// string
			if (! json_is_string(*pos))
				error("server: listen[] <host> must be a string");
			json_read_string_copy(pos, &self->host);
		} else
		if (str_is(&name, "port", 4))
		{
			// int
			if (! json_is_integer(*pos))
				error("server: listen[] <port> must be an integer");
			json_read_integer(pos, &self->host_port);
		} else
		if (str_is(&name, "auth", 4))
		{
			// bool
			if (! json_is_bool(*pos))
				error("server: listen[] <auth> must be a bool");
			json_read_bool(pos, &self->auth);
		} else
		if (str_is(&name, "tls", 3))
		{
			// bool
			if (! json_is_bool(*pos))
				error("server: listen[] <tls> must be a bool");
			json_read_bool(pos, &self->tls);
		} else
		{
			error("server: listen[] unknown listen option '%.*s'",
			      str_size(&name), str_of(&name));
		}
	}
	return self;
}
