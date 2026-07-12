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

void part_insert(Part*, Tr*, Timeline*, Row*);
bool part_upsert(Part*, Tr*, Iterator*, Timeline*, Row*);
void part_update(Part*, Tr*, Iterator*, Timeline*, Row*);
void part_delete(Part*, Tr*, Iterator*, Timeline*);
void part_delete_by(Part*, Tr*, Timeline*, Row*);
void part_follow(Part*, Row*, Columns*);
