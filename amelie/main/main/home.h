#pragma once

//
// amelie.
//
// Real-Time SQL Database.
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
home_open(Home* self)
{
	// create ~/.amelie
	char path[PATH_MAX];
	snprintf(path, sizeof(path), "%s/.amelie", getenv("HOME"));

	auto bootstrap = !fs_exists("%s", path);
	if (bootstrap)
		fs_mkdir(0755, "%s", path);

	// read ~/.amelie/login
	snprintf(path, sizeof(path), "%s/.amelie/login", getenv("HOME"));
	login_mgr_open(&self->login_mgr, path);
}

static inline void
home_sync(Home* self)
{
	char path[PATH_MAX];
	snprintf(path, sizeof(path), "%s/.amelie/login", getenv("HOME"));
	login_mgr_sync(&self->login_mgr, path);
}
