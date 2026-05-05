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

void catalog_drop_of(Catalog*, Tr*, Rel*);
bool catalog_drop(Catalog*, Tr*, RelType, Str*, Str*, bool, bool);
void catalog_rename_of(Catalog*, Tr*, Rel*, Str*, Str*);
bool catalog_rename(Catalog*, Tr*, RelType, Str*, Str*, Str*, Str*, bool);
void catalog_grant_of(Catalog*, Tr*, Rel*, Str*, bool, uint32_t);
bool catalog_grant(Catalog*, Tr*, Str*, Str*, Str*, bool, uint32_t);
void catalog_grant_rename_of(Catalog*, Tr*, Rel*, Str*, Str*);
