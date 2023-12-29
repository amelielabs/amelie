#pragma once

//
// indigo
//
// SQL OLTP database
//

int emit_string(Compiler*, Str*, bool);
int emit_expr(Compiler*, Target*, Ast*);
