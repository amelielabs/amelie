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

void engine_set_unlogged(Engine*, bool);
void engine_truncate(Engine*);
void engine_index_add(Engine*, IndexConfig*);
void engine_index_remove(Engine*, Str*);
