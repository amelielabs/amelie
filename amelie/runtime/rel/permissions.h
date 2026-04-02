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
	PERM_NONE            = 0,

	// System
	PERM_SYSTEM          = 1ul << 0,

	// Grants
	PERM_GRANT           = 1ul << 1,

	// DDL
	PERM_CREATE_USER     = 1ul << 2,
	PERM_CREATE_TOKEN    = 1ul << 3,
	PERM_CREATE_TABLE    = 1ul << 4,
	PERM_CREATE_BRANCH   = 1ul << 5,
	PERM_CREATE_FUNCTION = 1ul << 6,
	PERM_CREATE_STREAM   = 1ul << 7,

	// DML
	PERM_INSERT          = 1ul << 8,
	PERM_UPDATE          = 1ul << 9,
	PERM_DELETE          = 1ul << 10,

	// Query
	PERM_SELECT          = 1ul << 11,

	// UDF
	PERM_EXECUTE         = 1ul << 12,

	// Connections
	PERM_CONNECT         = 1ul << 13,
	PERM_SERVICE         = 1ul << 14,

	// all
	PERM_ALL             = UINT32_MAX,
};

int      permission_of(Str*, uint32_t*);
char*    permission_name_of(uint32_t);
uint32_t permission_next(uint32_t*);
uint32_t permission_validate(Str*, Str*, uint32_t, uint32_t);
