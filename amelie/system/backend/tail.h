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

typedef struct Tail  Tail;
typedef struct Tails Tails;

struct Tail
{
	Msg          msg;
	Msg          msg_cancel;
	bool         ready;
	bool         cancel;
	bool         wait;
	bool         error;

	HeapIterator it;
	Row*         key;
	Buf          data;

	Task*        part_task;
	Part*        part;
	Tail*        part_link;
	Task*        task;

	Tails*       tails;
	List         link;
};

void tail_init(Tail*, Tails*, Task*, Task*, Part*);
void tail_free(Tail*);
void tail_next(Tail*);
void tail_cancel(Tail*);
void tail_cancel_all(Part*);
void tail_resume(Part*);
