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

typedef struct MainCmd MainCmd;
typedef struct Main    Main;

typedef void (*MainCmdFunction)(Main*);

struct MainCmd
{
	MainCmdFunction function;
	const char*     name;
	const char*     description;
};

typedef enum
{
	MAIN_REMOTE,
	MAIN_REMOTE_PATH,
	MAIN_LOCAL
} MainAccess;

typedef enum
{
	MAIN_OPEN_HOME,
	MAIN_OPEN_CONFIGURE,
	MAIN_OPEN_REMOTE,
	MAIN_OPEN_LOCAL_RUNNING,
	MAIN_OPEN_LOCAL_NEW,
	MAIN_OPEN_LOCAL,
	MAIN_OPEN_ANY_NOOPTS,
	MAIN_OPEN_ANY
} MainOpen;

struct Main
{
	Console     console;
	BookmarkMgr bookmark_mgr;
	Endpoint    endpoint;
	MainAccess  access;
	amelie_t*   env;
	int         argc;
	char**      argv;
};

void main_init(Main*, int, char**);
void main_free(Main*);
void main_open(Main*, MainOpen, Opts*);
void main_close(Main*);
void main_runtime(void*, int, char**);

static inline void
main_advance(Main* self, int advance)
{
	self->argc -= advance;
	self->argv += advance;
	assert(self->argc >= 0);
}
