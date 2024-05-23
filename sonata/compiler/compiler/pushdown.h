#pragma once

//
// sonata.
//
// Real-Time SQL Database.
//

void pushdown(Compiler*, Ast*);
void pushdown_first(Compiler*, Ast*);
int  pushdown_recv(Compiler*, Ast*);
