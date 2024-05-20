#pragma once

//
// sonata.
//
// SQL Database for JSON.
//

void pushdown(Compiler*, Ast*);
void pushdown_first(Compiler*, Ast*);
int  pushdown_recv(Compiler*, Ast*);
