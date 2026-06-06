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

void catalog_read(Catalog*, char*);
void catalog_write(Catalog*, char*);
Buf* catalog_state(Catalog*, uint64_t, uint64_t);
