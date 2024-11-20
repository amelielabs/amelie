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

void part_insert(Part*, Tr*, bool, Row*);
void part_update(Part*, Tr*, Iterator*, Row*);
void part_delete(Part*, Tr*, Iterator*);
void part_delete_by(Part*, Tr*, Row*);
bool part_upsert(Part*, Tr*, Iterator*, Row*);
void part_ingest_secondary(Part*, Row*);
void part_ingest(Part*, Row*);
