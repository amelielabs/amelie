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

int    vector_compare(int, int, float*, float*);
void   vector_add(int, float*, float*, float*);
void   vector_sub(int, float*, float*, float*);
void   vector_mul(int, float*, float*, float*);
double vector_distance(int, const float*__restrict, const float*__restrict);

static inline int
vector_size(int dim)
{
	return dim * sizeof(float);
}
