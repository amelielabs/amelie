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

typedef struct PageHeader PageHeader;
typedef struct Page       Page;

struct PageHeader
{
	// 24 bytes (aligned by pointer)
	uint32_t crc;
	uint32_t size;
	uint32_t size_compressed;
	uint32_t order;
	uint32_t last;
	uint32_t padding;
} packed;

struct Page
{
	uint8_t* pointer;
};
