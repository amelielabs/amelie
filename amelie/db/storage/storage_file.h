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

size_t storage_create(Storage*, char*, uint8_t*, int);
size_t storage_open(Storage*, char*, int, Buf*);
