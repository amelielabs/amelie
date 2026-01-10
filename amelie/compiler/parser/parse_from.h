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

Target* parse_from_add(Stmt*, From*, LockId, Str*, bool);
void    parse_from(Stmt*, From*, LockId, bool);
