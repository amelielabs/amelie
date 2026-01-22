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

void heap_create(Heap*, File*, Storage*, Id*, int, Encoding*);
void heap_open(Heap*, Storage*, Id*, int, Encoding*);
