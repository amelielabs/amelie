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

bool cascade_user_rename(Catalog*, Tr*, Str*, Str*, bool);

bool catalog_deps_add(Buf*, Rel*);
int  catalog_deps(Catalog*, Rel*, Buf*);
void catalog_deps_drop(Catalog*, Tr*, Buf*);
bool catalog_deps_validate(Catalog*, Rel*, bool);
