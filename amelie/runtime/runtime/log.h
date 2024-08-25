#pragma once

//
// amelie.
//
// Real-Time SQL Database.
//

typedef void (*LogFunction)(void* arg,
                            const char* file,
                            const char* function, int line,
                            const char* prefix,
                            const char* text);
