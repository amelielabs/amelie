#pragma once

//
// indigo
//
// SQL OLTP database
//

#define likely(expr)      __builtin_expect(!! (expr), 1)
#define unlikely(expr)    __builtin_expect(!! (expr), 0)
#define hot               __attribute__((hot))
#define always_inline     __attribute__((always_inline))
#define packed            __attribute__((packed))
#define no_return         __attribute__((noreturn))
#define fallthrough       __attribute__((fallthrough));
#define auto              __auto_type
#define unused(name)     (void)name

#define source_file       __FILE__
#define source_function   __func__
#define source_line       __LINE__

#define container_of(ptr, type, field) \
	((type*)((char*)(ptr) - __builtin_offsetof(type, field)))
