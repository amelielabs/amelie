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

typedef struct Receiver Receiver;

struct Receiver
{
	List       list;
	int        list_count;
	RecoverIf* recover_if;
	void*      recover_if_arg;
	Db*        db;
	Task       task;
};

void receiver_init(Receiver*, Db*, RecoverIf*, void*);
void receiver_free(Receiver*);
void receiver_start(Receiver*);
void receiver_stop(Receiver*);
void receiver_send(Receiver*, Client*);
