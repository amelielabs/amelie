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

void catalog_read(Catalog*);
void catalog_write(Catalog*);
Buf* catalog_write_prepare(Catalog*, uint64_t, uint64_t);
