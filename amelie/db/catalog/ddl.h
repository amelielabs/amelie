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
	// generic
	DDL_DROP,
	DDL_RENAME,
	DDL_GRANT,
	DDL_DESCRIBE,

	// user
	DDL_USER_CREATE,
	DDL_USER_DROP,
	DDL_USER_RENAME,
	DDL_USER_REVOKE_TOKEN,
	DDL_USER_DESCRIBE,

	// table
	DDL_TABLE_CREATE,
	DDL_TABLE_TRUNCATE,
	DDL_TABLE_SET_IDENTITY,
	DDL_TABLE_COLUMN_ADD,
	DDL_TABLE_COLUMN_DROP,
	DDL_TABLE_COLUMN_RENAME,
	DDL_TABLE_COLUMN_SET_DEFAULT,

	// table storage
	DDL_TABLE_STORAGE_ADD,
	DDL_TABLE_STORAGE_DROP,
	DDL_TABLE_STORAGE_PAUSE,

	// index
	DDL_INDEX_CREATE,
	DDL_INDEX_DROP,
	DDL_INDEX_RENAME,

	// clone
	DDL_CLONE_CREATE,

	// udf
	DDL_UDF_CREATE,

	// topic
	DDL_TOPIC_CREATE,

	// subscription
	DDL_SUB_CREATE,
};

enum
{
	DDL_IF_NOT_EXISTS         = 1 << 0,
	DDL_IF_EXISTS             = 1 << 1,
	DDL_IF_COLUMN_NOT_EXISTS  = 1 << 2,
	DDL_IF_COLUMN_EXISTS      = 1 << 3,
	DDL_IF_STORAGE_NOT_EXISTS = 1 << 4,
	DDL_IF_STORAGE_EXISTS     = 1 << 5
};

static inline bool
ddl_if_not_exists(int flags)
{
	return (flags & DDL_IF_NOT_EXISTS) > 0;
}

static inline bool
ddl_if_exists(int flags)
{
	return (flags & DDL_IF_EXISTS) > 0;
}

static inline bool
ddl_if_column_exists(int flags)
{
	return (flags & DDL_IF_COLUMN_EXISTS) > 0;
}

static inline bool
ddl_if_column_not_exists(int flags)
{
	return (flags & DDL_IF_COLUMN_NOT_EXISTS) > 0;
}

static inline bool
ddl_if_storage_not_exists(int flags)
{
	return (flags & DDL_IF_STORAGE_NOT_EXISTS) > 0;
}

static inline bool
ddl_if_storage_exists(int flags)
{
	return (flags & DDL_IF_STORAGE_EXISTS) > 0;
}

static inline int
ddl_of(uint8_t* op)
{
	// [cmd, ...]
	int64_t cmd;
	unpack_array(&op);
	unpack_int(&op, &cmd);
	return cmd;
}
