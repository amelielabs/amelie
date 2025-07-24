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

enum
{
	MSG_OK,
	MSG_ERROR,
	MSG_CLIENT,

	// rpc
	RPC_START,
	RPC_STOP,
	RPC_RESOLVE,

	RPC_LOCK,
	RPC_UNLOCK,
	RPC_CHECKPOINT,

	RPC_SYNC_USERS,
	RPC_SHOW_METRICS
};
