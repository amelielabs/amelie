#pragma once

//
// sonata.
//
// Real-Time SQL Database.
//

enum
{
	MSG_OK,
	MSG_READY,
	MSG_ERROR,
	MSG_CLIENT,

	MSG_SNAPSHOT_ROW,

	// rpc
	RPC_RECOVER,
	RPC_STOP,
	RPC_RESOLVE,

	RPC_BEGIN,
	RPC_PREPARE,
	RPC_COMMIT,
	RPC_ABORT,

	RPC_LOCK,
	RPC_UNLOCK,
	RPC_SYNC,

	RPC_SHOW_USERS,
	RPC_SHOW_NODES
};
