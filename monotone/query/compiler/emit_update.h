#pragma once

//
// monotone
//
// SQL OLTP database
//

void emit_update_target(Compiler*, Target*, Ast*);
void emit_update(Compiler*, Ast*);
