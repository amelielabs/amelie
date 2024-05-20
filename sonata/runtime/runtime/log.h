#pragma once

//
// sonata.
//
// SQL Database for JSON.
//

typedef void (*LogFunction)(void* arg,
                            const char* file,
                            const char* function, int line,
                            const char* fmt, ...);
