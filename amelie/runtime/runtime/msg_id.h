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
	MSG_READY,
	MSG_ERROR,
	MSG_CLIENT,

	MSG_SNAPSHOT_ROW,

	// rpc
	RPC_START,
	RPC_STOP,
	RPC_RESOLVE,

	RPC_BEGIN,
	RPC_PREPARE,
	RPC_COMMIT,
	RPC_ABORT,

	RPC_BUILD,

	RPC_LOCK,
	RPC_UNLOCK,
	RPC_CHECKPOINT,
	RPC_SYNC_USERS,
	RPC_SYNC,

	RPC_SHOW_USERS,
	RPC_SHOW_REPLICAS,
	RPC_SHOW_REPL,
	RPC_SHOW_METRICS
};
