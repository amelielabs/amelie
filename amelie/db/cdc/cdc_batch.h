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

typedef struct CdcBatch CdcBatch;

struct CdcBatch
{
	uint64_t lsn;
	uint32_t request_size;
	uint8_t* request;
	List*    list;
	Rel*     user;
};
