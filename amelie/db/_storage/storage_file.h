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

typedef struct StorageHeader StorageHeader;

#define STORAGE_MAGIC   0x20849610
#define STORAGE_VERSION 0

struct StorageHeader
{
	uint32_t crc;
	uint32_t magic;
	uint32_t version;
	uint8_t  type;
	uint8_t  compression;
	uint32_t size_page;
	uint32_t size_meta;
	uint32_t count;
} packed;

size_t storage_create(Storage*, char*, uint8_t*, int);
size_t storage_open(Storage*, char*, int, Buf*);
