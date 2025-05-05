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

typedef struct Record    Record;
typedef struct RecordCmd RecordCmd;

typedef enum
{
	// DML
	CMD_REPLACE,
	CMD_DELETE,
	// DDL
	CMD_SCHEMA_CREATE,
	CMD_SCHEMA_DROP,
	CMD_SCHEMA_RENAME,
	CMD_TABLE_CREATE,
	CMD_TABLE_DROP,
	CMD_TABLE_RENAME,
	CMD_TABLE_TRUNCATE,
	CMD_TABLE_SET_UNLOGGED,
	CMD_TABLE_COLUMN_RENAME,
	CMD_TABLE_COLUMN_ADD,
	CMD_TABLE_COLUMN_DROP,
	CMD_TABLE_COLUMN_SET_DEFAULT,
	CMD_TABLE_COLUMN_SET_IDENTITY,
	CMD_TABLE_COLUMN_SET_STORED,
	CMD_TABLE_COLUMN_SET_RESOLVED,
	CMD_INDEX_CREATE,
	CMD_INDEX_DROP,
	CMD_INDEX_RENAME,
	CMD_PROC_CREATE,
	CMD_PROC_DROP,
	CMD_PROC_RENAME
} Cmd;

struct Record
{
	uint32_t crc;
	uint64_t lsn;
	uint32_t size;
	uint32_t count;
	uint32_t ops;
} packed;

struct RecordCmd
{
	uint8_t  cmd;
	uint32_t size;
	uint32_t crc;
	uint32_t partition;
} packed;

static inline RecordCmd*
record_cmd(Record* self)
{
	return (RecordCmd*)((uint8_t*)self + sizeof(*self));
}

static inline uint8_t*
record_data(Record* self)
{
	return (uint8_t*)self + sizeof(*self) + (self->count * sizeof(RecordCmd));
}

static inline bool
record_cmd_is_dml(RecordCmd* self)
{
	return self->cmd == CMD_REPLACE || self->cmd == CMD_DELETE;
}

static inline bool
record_validate(Record* self)
{
	auto size = self->count * sizeof(RecordCmd);
	auto crc  = global()->crc(0, (uint8_t*)self + sizeof(*self), size);
	crc = global()->crc(crc, &self->lsn, sizeof(*self) - sizeof(self->crc));
	return crc == self->crc;
}

static inline bool
record_validate_cmd(RecordCmd* self, uint8_t* data)
{
	uint32_t crc = global()->crc(0, data, self->size);
	return crc == self->crc;
}
