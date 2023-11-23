#pragma once

//
// monotone
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

	MSG_SNAPSHOT_STORAGE,
	MSG_SNAPSHOT_ROW,
	MSG_WAL_WRITE,

	RPC_STOP,
	RPC_RESOLVE,

	RPC_USER_CACHE_SYNC,
	RPC_USER_CREATE,
	RPC_USER_DROP,
	RPC_USER_ALTER,
	RPC_USER_SHOW,

	RPC_COMPACT,

	RPC_WAL_GC,
	RPC_WAL_SNAPSHOT,
	RPC_WAL_WRITE,
	RPC_WAL_STATUS,

	RPC_REQUEST,
	RPC_REQUEST_COMMIT,
	RPC_REQUEST_ABORT,

	RPC_CAT_LOCK_REQ,
	RPC_CAT_LOCK,
	RPC_CAT_UNLOCK,

	RPC_STORAGE_ATTACH,
	RPC_STORAGE_DETACH
};
