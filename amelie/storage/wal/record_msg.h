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
typedef struct Db        Db;

struct RecordMsg
{
	Msg    msg;
	Buf*   msg_buf;
	Db*    db;
	void*  arg;
	Record record[];
};
