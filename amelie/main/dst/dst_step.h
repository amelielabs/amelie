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

void dst_step(DstUser*);
void dst_step_ddl(DstUser*);
void dst_step_ddl_user(DstUser*);
