#pragma once

//
// sonata.
//
// SQL Database for JSON.
//

typedef void (*ScanFunction)(Compiler*, void*);

void scan(Compiler*, Target*, Ast*, Ast*, Ast*,
          ScanFunction, void*);
