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

typedef struct RecordMsg RecordMsg;

struct RecordMsg
{
	Msg      msg;
	Buf*     msg_buf;
	void*    arg;
	Uuid     instance_id;
	uint64_t record_id;
	Record   record[];
};
