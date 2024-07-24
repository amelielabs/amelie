#pragma once

//
// sonata.
//
// Real-Time SQL Database.
//

typedef void (*ScanFunction)(Compiler*, void*);

void scan(Compiler*, Target*, Ast*, Ast*, Ast*,
          ScanFunction, void*);
