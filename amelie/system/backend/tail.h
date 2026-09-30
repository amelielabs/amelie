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
	bool         wait;
	bool         shutdown;
	bool         ready;
	Event        on_complete;
	Event        on_cancel;
	// iterator
	HeapIterator it;
	Row*         key;
	Buf          data;
	// partition state
	Task*        part_task;
	Part*        part;
	Tail*        part_link;
	Tails*       tails;
	List         link;
};

void tail_init(Tail*, Tails*, Part*);
void tail_free(Tail*);
void tail_next(Tail*);
void tail_cancel(Tail*);
void tail_cancel_all(Part*);
void tail_resume_all(Part*);
