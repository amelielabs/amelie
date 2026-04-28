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

bool udf_mgr_create(Catalog*, Tr*, UdfConfig*, bool);
void udf_mgr_replace_validate(Catalog*, Tr*, UdfConfig*, Udf*);
void udf_mgr_replace(Catalog*, Tr*, Udf*, Udf*);
bool udf_mgr_drop(Catalog*, Tr*, Str*, Str*, bool);
void udf_mgr_drop_of(Catalog*, Tr*, Udf*);
bool udf_mgr_rename(Catalog*, Tr*, Str*, Str*, Str*, Str*, bool);
bool udf_mgr_grant(Catalog*, Tr*, Str*, Str*, Str*, bool, uint32_t, bool);
