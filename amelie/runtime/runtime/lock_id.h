#pragma once

//
// amelie.
//
// Real-Time SQL Database.
//

enum
{
	LOCK_NONE          = 0,
	LOCK               = 1 << 1,
	LOCK_EXCLUSIVE     = 1 << 2,
	LOCK_REF           = 1 << 3,
	LOCK_REF_EXCLUSIVE = 1 << 4,
	LOCK_CHECKPOINT    = 1 << 5
};
