#pragma once

//
// amelie.
//
// Real-Time SQL OLTP Database.
//
// Copyright (c) 2024 Dmitry Simonenko.
// Copyright (c) 2024 Amelie Labs.
//
// AGPL-3.0 Licensed.
//

/*
   Decimal (64bit)

   Bits  0-59: signed 60-bit integer (2^53 - 1 max)
   Bits 60-63: 4-bit scale (0 to 15)
*/

#define DECIMAL_MAX_SCALE 15
#define DECIMAL_MIN       -9007199254740991LL
#define DECIMAL_MAX        9007199254740991LL

always_inline static inline uint64_t
decimal_set(int64_t value, uint32_t scale)
{
	return (((uint64_t)scale & 0x0FULL) << 60) |
	        ((uint64_t)value & 0x0FFFFFFFFFFFFFFFULL);
}

always_inline static inline uint32_t
decimal_scale(uint64_t self)
{
    return (uint32_t)(self >> 60);
}

always_inline static inline int64_t
decimal_value(uint64_t self)
{
    int64_t val = (int64_t)(self << 4);
    return val >> 4;
}

uint64_t decimal_add(uint64_t, uint64_t);
uint64_t decimal_addei(uint64_t, int64_t);
uint64_t decimal_sub(uint64_t, uint64_t);
uint64_t decimal_subei(uint64_t, int64_t);
uint64_t decimal_subie(int64_t, uint64_t);
uint64_t decimal_mul(uint64_t, uint64_t);
uint64_t decimal_mulei(uint64_t, int64_t);
uint64_t decimal_div(uint64_t, uint64_t);
uint64_t decimal_divei(uint64_t, int64_t);
uint64_t decimal_divie(int64_t, uint64_t);
uint64_t decimal_neg(uint64_t);
uint64_t decimal_modei(uint64_t, int64_t);
uint64_t decimal_set_str(Str*);
uint64_t decimal_set_int(int, int64_t);
uint64_t decimal_set_double(double);
uint64_t decimal_set_double_round(int, double);
int      decimal_get(uint64_t, char*, int);
int64_t  decimal_get_int(uint64_t);
double   decimal_get_double(uint64_t);
int      decimal_compare(uint64_t, uint64_t);
