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

typedef struct Daemon Daemon;

typedef enum
{
	DAEMON_START,
	DAEMON_STOP,
	DAEMON_OTHER
} DaemonCmd;

struct Daemon
{
	DaemonCmd cmd;
	bool      daemon;
	char*     directory;
	int       directory_fd;
	int       argc;
	char**    argv;
};

void daemon_init(Daemon*);
int  daemon_main(Daemon*, int, char**);
void daemon_wait_for_signal(void);
