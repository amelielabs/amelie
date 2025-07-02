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
	DDL_TABLE_SET_UNLOGGED,
	DDL_TABLE_COLUMN_ADD,
	DDL_TABLE_COLUMN_DROP,
	DDL_TABLE_COLUMN_SET_DEFAULT,
	DDL_TABLE_COLUMN_SET_IDENTITY,
	DDL_TABLE_COLUMN_SET_STORED,
	DDL_TABLE_COLUMN_SET_RESOLVED,
	DDL_TABLE_COLUMN_RENAME,

	// index
	DDL_INDEX_CREATE,
	DDL_INDEX_DROP,
	DDL_INDEX_RENAME
};

// schema
int ddl_schema_create(Buf*, SchemaConfig*); 
int ddl_schema_drop(Buf*, Str*); 
int ddl_schema_rename(Buf*, Str*, Str*); 

// table
int ddl_table_create(Buf*, TableConfig*); 
int ddl_table_drop(Buf*, Str*, Str*); 
int ddl_table_rename(Buf*, Str*, Str*, Str*, Str*);
int ddl_table_truncate(Buf*, Str*, Str*);
int ddl_table_set_unlogged(Buf*, Str*, Str*, bool);
int ddl_table_column_add(Buf*, Str*, Str*, Column*);
int ddl_table_column_drop(Buf*, Str*, Str*, Str*);
int ddl_table_column_set(Buf*, int, Str*, Str*, Str*, Str*, Str*);
int ddl_table_column_rename(Buf*, Str*, Str*, Str*, Str*);

// index
int ddl_index_create(Buf*, Str*, Str*, IndexConfig*); 
int ddl_index_drop(Buf*, Str*, Str*, Str*); 
int ddl_index_rename(Buf*, Str*, Str*, Str*, Str*); 

// execute
int ddl_execute(Db*, Tr*, uint8_t**);
