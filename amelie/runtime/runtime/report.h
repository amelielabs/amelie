#pragma once

//
// sonata.
//
// Real-Time SQL Database.
//

void
report(const char* file,
       const char* function, int line,
       const char* prefix,
       const char* fmt, ...);

void no_return
report_throw(const char* file,
             const char* function, int line,
             int         code,
             const char* fmt, ...);
