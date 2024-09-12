#pragma once

//
// amelie.
//
// Real-Time SQL Database.
//

enum
{
	MSG_OK,
	MSG_READY,
	MSG_ERROR,

	MSG_SNAPSHOT_ROW,

	// rpc
	RPC_STOP,

	RPC_BEGIN,
	RPC_PREPARE,
	RPC_COMMIT,
	RPC_ABORT,

	RPC_BUILD
};
