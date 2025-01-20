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

// 0001-01-01
#define DATE_MIN (1721426)

// 9999-12-31
#define DATE_MAX (5373484)

int  date_set(Str*);
int  date_set_julian(int64_t);
int  date_set_gregorian(int, int, int);
void date_get_gregorian(int64_t, int*, int*, int*);
int  date_get(int64_t, char*, int);
int  date_add(int64_t, int);
int  date_sub(int64_t, int);
int  date_extract(int64_t, Str*);
