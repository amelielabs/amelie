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
typedef struct User      User;

struct RecordMsg
{
	Msg    msg;
	Buf*   msg_buf;
	User*  user;
	void*  arg;
	Record record[];
};
