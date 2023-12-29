#pragma once

//
// indigo
//
// SQL OLTP database
//

typedef void (*LogFunction)(void* arg,
                            const char* file,
                            const char* function, int line,
                            const char* fmt, ...);
