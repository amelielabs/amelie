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

typedef struct Home Home;

struct Home
{
	LoginMgr login_mgr;
};

static inline void
home_init(Home* self)
{
	login_mgr_init(&self->login_mgr);
}

static inline void
home_free(Home* self)
{
	login_mgr_free(&self->login_mgr);
}

static inline void
home_set_path(char* path, int path_size, char* fmt, ...)
{
	// use AMELIE_HOME or ~/.amelie by default
	int  size = 0;
	auto home = getenv("AMELIE_HOME");
	if (home)
	{
		size = snprintf(path, path_size, "%s/", home);
	} else
	{
		home = getenv("HOME");
		size = snprintf(path, path_size, "%s/.amelie/", home);
	}
	va_list args;
	va_start(args, fmt);
	size += vsnprintf(path + size, path_size - size, fmt, args);
	va_end(args);
}

static inline void
home_open(Home* self)
{
	// use AMELIE_HOME or ~/.amelie by default
	char path[PATH_MAX];
	home_set_path(path, sizeof(path), "");

	auto bootstrap = !fs_exists("%s", path);
	if (bootstrap)
		fs_mkdir(0755, "%s", path);

	// read $AMELIE_HOME/login
	home_set_path(path, sizeof(path), "login");
	login_mgr_open(&self->login_mgr, path);
}

static inline void
home_sync(Home* self)
{
	// save logins
	char path[PATH_MAX];
	home_set_path(path, sizeof(path), "login");
	login_mgr_sync(&self->login_mgr, path);
}
