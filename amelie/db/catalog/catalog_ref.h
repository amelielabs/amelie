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

Rel* catalog_cdc_ref(Catalog*, User*, Str*, Str*, Uuid**);
void catalog_cdc_unref(Catalog*, Uuid*);
