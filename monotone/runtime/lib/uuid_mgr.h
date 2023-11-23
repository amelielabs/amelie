#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct Uuid    Uuid;
typedef struct UuidMgr UuidMgr;

#define UUID_SZ 37

struct Uuid
{
	uint64_t a;
	uint64_t b;
};

struct UuidMgr
{
	Spinlock lock;
	uint64_t seed[2];
};

void uuid_init(Uuid*);
void uuid_mgr_init(UuidMgr*);
void uuid_mgr_free(UuidMgr*);
void uuid_mgr_open(UuidMgr*);
void uuid_mgr_generate(UuidMgr*, Uuid*);
uint64_t
uuid_mgr_random(UuidMgr*);

static inline bool
uuid_empty(Uuid* self)
{
	return self->a == self->b == 0;
}

static inline bool
uuid_compare(Uuid *l, Uuid *r)
{
	return l->a == r->a && l->b == r->b;
}

void uuid_to_string(Uuid*, char*, int);
void uuid_from_string(Uuid*, Str*);
int  uuid_from_string_nothrow(Uuid*, Str*);
