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

void emit_select_on_match(Compiler*, Targets*, void*);
void emit_select_on_match_aggregate(Compiler*, Targets*, void*);
void emit_select_on_match_aggregate_empty(Compiler*, Targets*, void*);

int  emit_select_order_by_data(Compiler*, AstSelect*, bool);
int  emit_select_union(Compiler*, AstSelect*);
int  emit_select(Compiler*, Ast*);
