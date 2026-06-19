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

typedef struct Record Record;

enum
{
	RECORD_UTILITY = 1
};

struct Record
{
	uint32_t crc;
	uint32_t size;
	uint8_t  flags;
	uint64_t lsn;
	uint64_t tsn;
} packed;

static inline uint8_t*
record_data(Record* self)
{
	return (uint8_t*)self + sizeof(Record);
}

static inline uint32_t
record_data_size(Record* self)
{
	return self->size - sizeof(Record);
}

static inline bool
record_validate(Record* self)
{
	auto crc = runtime()->crc(0, &self->size, self->size - sizeof(self->crc));
	return crc == self->crc;
}
