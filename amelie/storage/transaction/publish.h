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

typedef struct Publish Publish;

struct Publish
{
	Msg      msg;
	Buf*     msg_buf;
	Uuid     db;
	void*    db_arg;
	uint64_t lsn;
};

static inline Publish*
publish_of(Msg* self)
{
	return (Publish*)self;
}
