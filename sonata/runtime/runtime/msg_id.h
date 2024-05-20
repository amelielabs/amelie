#pragma once

//
// sonata.
//
// SQL Database for JSON.
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

	RPC_CTL,
	RPC_LOCK,
	RPC_UNLOCK,
	RPC_SYNC,

	RPC_USER_SHOW

#if 0
	MSG_SNAPSHOT_ROW,
	MSG_WAL_WRITE,

	RPC_COMPACT,

	RPC_SNAPSHOT_WRITE,

	RPC_WAL_GC,
	RPC_WAL_SNAPSHOT,
	RPC_WAL_WRITE,
	RPC_WAL_STATUS,
#endif
};
