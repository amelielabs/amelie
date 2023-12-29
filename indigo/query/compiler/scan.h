#pragma once

//
// indigo
//
// SQL OLTP database
//

typedef void (*ScanFunction)(Compiler*, void*);

void scan(Compiler*, Target*, Ast*, Ast*, Ast*,
          ScanFunction, void*);
