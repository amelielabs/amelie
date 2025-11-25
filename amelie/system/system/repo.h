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

typedef struct Repo Repo;

struct Repo
{
	int  fd;
	bool bootstrap;
};

void repo_init(Repo*);
void repo_open(Repo*, char*, int, char**);
void repo_close(Repo*);
