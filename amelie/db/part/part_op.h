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

void part_insert(Part*, Tr*, bool, Branch*, Row*);
bool part_upsert(Part*, Tr*, Iterator*, Branch*, Row*);
void part_update(Part*, Tr*, Iterator*, Branch*, Row*);
void part_delete(Part*, Tr*, Iterator*, Branch*);
void part_delete_by(Part*, Tr*, Branch*, Row*);
void part_follow(Part*, Row*, Columns*);
bool part_apply(Part*, Index*);
