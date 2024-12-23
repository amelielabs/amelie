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

typedef void (*ScanFunction)(Compiler*, Targets*, void*);

void scan(Compiler*, Targets*, Ast*, Ast*, Ast*,
          ScanFunction, void*);
