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

Row* row_create_key(Buf*, Keys*, Value*, int);
Row* row_create(Part*, Columns*, Value*, Value*, Value*);
Row* row_update(Part*, Columns*, Row*, Value*, int);
