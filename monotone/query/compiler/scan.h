#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef void (*ScanFunction)(Compiler*, void*);

void scan(Compiler*, Target*, Ast*, Ast*, Ast*,
          ScanFunction, void*);
