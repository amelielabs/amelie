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

typedef uint32_t (*CrcFunction)(uint32_t    crc,
                                const void* data,
                                int         data_size);

uint32_t crc32(uint32_t, const void*, int);
bool     crc32_sse_supported(void);
uint32_t crc32_sse(uint32_t, const void*, int);
