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

typedef struct Decimal Decimal;

struct Decimal
{
	int64_t value;
	uint8_t scale;
} packed;

static inline void
decimal_init(Decimal* self)
{
	self->value = 0;
	self->scale = 0;
}

// casting
void decimal_set(Decimal*, Str*);
void decimal_set_int(Decimal*, int, int64_t);
void decimal_set_double(Decimal*, int, double);

// add
void decimal_add(Decimal*, Decimal*, Decimal*);
void decimal_addei(Decimal*, Decimal*, int64_t);
void decimal_addef(double*, Decimal*, double);

// sub
void decimal_sub(Decimal*, Decimal*, Decimal*);
void decimal_subei(Decimal*, Decimal*, int64_t);
void decimal_subie(Decimal*, int64_t, Decimal*);
void decimal_subef(double*, Decimal*, double);
void decimal_subfe(double*, double, Decimal*);

// mul
void decimal_mul(Decimal*, Decimal*, Decimal*);
void decimal_mulei(Decimal*, Decimal*, int64_t);
void decimal_mulef(double*, Decimal*, double);

// div
void decimal_div(Decimal*, Decimal*, Decimal*);
void decimal_divei(Decimal*, Decimal*, int64_t);
void decimal_divie(Decimal*, int64_t, Decimal*);
void decimal_divef(double*, Decimal*, double);
void decimal_divfe(double*, double, Decimal*);

// mod
void decimal_modei(Decimal*, Decimal*, int64_t);
