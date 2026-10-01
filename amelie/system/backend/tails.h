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

typedef struct Tails Tails;

struct Tails
{
	Tail*   tails;
	int     tails_count;
	List    ready;
	Buf     key;
	Event   notify;
	Iov     iov;
	Client* client;
	Task*   task;
};

void tails_init(Tails*, Task*, Client*);
void tails_free(Tails*);
void tails_create(Tails*, Parts*, Str*);
void tails_run(Tails*);
