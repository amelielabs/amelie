#pragma once

//
// sonata.
//
// SQL Database for JSON.
//

int emit_string(Compiler*, Str*, bool);
int emit_expr(Compiler*, Target*, Ast*);
