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

void catalog_validate_udfs(Catalog*, Str*, Str*);
bool cascade_user_rename(Catalog*, Tr*, Str*, Str*, bool);

bool cascade_deps_add(Buf*, Rel*);
int  cascade_deps(Catalog*, Rel*, Buf*);
void cascade_drop(Catalog*, Tr*, Buf*);
