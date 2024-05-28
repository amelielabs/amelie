#pragma once

//
// sonata.
//
// SQL Database for JSON.
//

void
report(const char* file,
       const char* function, int line,
       int         code,
       const char* prefix,
       bool        error,
       const char* fmt, ...);
