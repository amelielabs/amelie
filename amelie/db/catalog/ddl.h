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
	// schema
	DDL_SCHEMA_CREATE,
	DDL_SCHEMA_DROP,
	DDL_SCHEMA_RENAME,

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
	DDL_TABLE_COLUMN_SET_IDENTITY,
	DDL_TABLE_COLUMN_SET_STORED,
	DDL_TABLE_COLUMN_SET_RESOLVED,

	// index
	DDL_INDEX_CREATE,
	DDL_INDEX_DROP,
	DDL_INDEX_RENAME
};

enum
{
	DDL_IF_NOT_EXISTS        = 1,
	DDL_IF_EXISTS            = 2,
	DDL_IF_COLUMN_NOT_EXISTS = 4,
	DDL_IF_COLUMN_EXISTS     = 8
};

always_inline static inline bool
ddl_if_not_exists(int flags)
{
	return (flags & DDL_IF_NOT_EXISTS) > 0;
}

always_inline static inline bool
ddl_if_exists(int flags)
{
	return (flags & DDL_IF_EXISTS) > 0;
}

always_inline static inline bool
ddl_if_column_exists(int flags)
{
	return (flags & DDL_IF_COLUMN_EXISTS) > 0;
}

always_inline static inline bool
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
