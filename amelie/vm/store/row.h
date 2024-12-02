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

Row* row_create(Columns*, Value*, int);
Row* row_create_key(Keys*, Value*);
Row* row_update(Row*, Columns*, Value*, int);
void row_update_values(Columns*, Keys*, Value*, Value*, SetMeta*);
