#pragma once

//
// indigo
//
// SQL OLTP database
//

void emit_select_on_match(Compiler*, void*);
void emit_select_on_match_set(Compiler*, void*);
void emit_select_on_match_group_target(Compiler*, void*);
void emit_select_on_match_group(Compiler*, void*);

int  emit_select_expr(Compiler*, AstSelect*);
int  emit_select_order_by_data(Compiler*, AstSelect*, bool*);
int  emit_select_merge(Compiler*, AstSelect*);
int  emit_select(Compiler*, Ast*, bool);
