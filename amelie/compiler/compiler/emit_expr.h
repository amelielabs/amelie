#pragma once

//
// sonata.
//
// Real-Time SQL Database.
//

int emit_string(Compiler*, Str*, bool);
int emit_expr(Compiler*, Target*, Ast*);
