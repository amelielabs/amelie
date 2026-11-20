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

typedef struct Publisher Publisher;

struct Publisher
{
	Mailbox  queue;
	Portal   portal;
	Request  req;
	void*    session;
	int64_t  worker_id;
	Buf      buf;
	Compute* compute;
};

void publisher_init(Publisher*, Compute*);
void publisher_free(Publisher*);
void publisher_start(Publisher*);
void publisher_stop(Publisher*);
void publisher_add(Publisher*, Msg*);
