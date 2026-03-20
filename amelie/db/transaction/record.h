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
	CMD_DDL,
	CMD_DDL_CREATE_INDEX
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
	Uuid     id;
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

static inline void
record_cmd_skip(RecordCmd* self, uint8_t** pos)
{
	auto end = *pos + self->size;
	while (*pos < end)
		*pos += row_size((Row*)*pos);
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
	auto crc  = runtime()->crc(0, (uint8_t*)self + sizeof(*self), size);
	crc = runtime()->crc(crc, &self->lsn, sizeof(*self) - sizeof(self->crc));
	return crc == self->crc;
}

static inline bool
record_validate_cmd(RecordCmd* self, uint8_t* data)
{
	uint32_t crc = runtime()->crc(0, data, self->size);
	return crc == self->crc;
}
