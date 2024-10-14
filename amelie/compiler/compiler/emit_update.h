#pragma once

//
// amelie.
//
// Real-Time SQL OLTP Database.
//
// Copyright (c) 2024 Dmitry Simonenko.
// AGPL-3.0 Licensed.
//

void emit_update_target(Compiler*, Target*, Ast*);
void emit_update(Compiler*, Ast*);
