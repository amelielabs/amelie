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
	// tier
	DDL_TIER_CREATE,
	DDL_TIER_DROP,
	DDL_TIER_RENAME,

	// database
	DDL_DB_CREATE,
	DDL_DB_DROP,
	DDL_DB_RENAME,

	// table
	DDL_TABLE_CREATE,
	DDL_TABLE_DROP,
	DDL_TABLE_RENAME,
	DDL_TABLE_TRUNCATE,
	DDL_TABLE_SET_IDENTITY,
	DDL_TABLE_SET_UNLOGGED,
	DDL_TABLE_COLUMN_ADD,
	DDL_TABLE_COLUMN_DROP,
	DDL_TABLE_COLUMN_RENAME,
	DDL_TABLE_COLUMN_SET_DEFAULT,
	DDL_TABLE_COLUMN_SET_STORED,
	DDL_TABLE_COLUMN_SET_RESOLVED,

	// index
	DDL_INDEX_CREATE,
	DDL_INDEX_DROP,
	DDL_INDEX_RENAME,

	// udf
	DDL_UDF_CREATE,
	DDL_UDF_REPLACE,
	DDL_UDF_DROP,
	DDL_UDF_RENAME
};

enum
{
	DDL_IF_NOT_EXISTS        = 1 << 0,
	DDL_IF_EXISTS            = 1 << 1,
	DDL_IF_COLUMN_NOT_EXISTS = 1 << 2,
	DDL_IF_COLUMN_EXISTS     = 1 << 3
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

static inline int
ddl_of(uint8_t* op)
{
	// [cmd, ...]
	int64_t cmd;
	json_read_array(&op);
	json_read_integer(&op, &cmd);
	return cmd;
}
