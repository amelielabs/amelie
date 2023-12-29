#pragma once

//
// indigo
//
// SQL OLTP database
//

enum
{
	MSG_OK,
	MSG_READY,
	MSG_ERROR,
	MSG_OBJECT,

	MSG_CONNECT,
	MSG_DISCONNECT,

	MSG_CLIENT,
	MSG_NATIVE,

	MSG_AUTH,
	MSG_AUTH_REPLY,
	MSG_AUTH_COMPLETE,

	MSG_COMMAND,

	MSG_SNAPSHOT_ROW,
	MSG_WAL_WRITE,

	RPC_STOP,
	RPC_RESOLVE,

	RPC_CTL,

	RPC_USER_CACHE_SYNC,
	RPC_USER_SHOW,

	RPC_COMPACT,

	RPC_SNAPSHOT_WRITE,

	RPC_WAL_GC,
	RPC_WAL_SNAPSHOT,
	RPC_WAL_WRITE,
	RPC_WAL_STATUS,

	RPC_REQUEST,
	RPC_REQUEST_COMMIT,
	RPC_REQUEST_ABORT,

	RPC_SESSION_LOCK,
	RPC_SESSION_UNLOCK
};
