#pragma once

//
// monotone
//
// SQL OLTP database
//

void emit_select_on_match(Compiler*, void*);
void emit_select_on_match_set(Compiler*, void*);
int  emit_select_expr(Compiler*, AstSelect*);
int  emit_select_order_by(Compiler*, Ast*, bool*);
int  emit_select(Compiler*, Ast*, bool);
