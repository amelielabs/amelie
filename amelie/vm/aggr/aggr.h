#pragma once

//
// amelie.
//
// Real-Time SQL Database.
//

typedef struct AggrIf AggrIf;
typedef struct Aggr   Aggr;

struct AggrIf
{
	Aggr* (*create)(AggrIf*, Value*);
	void  (*free)(Aggr*);
	void  (*state_create)(Aggr*, uint8_t*);
	void  (*state_free)(Aggr*, uint8_t*);
	int   (*state_size)(Aggr*);
	void  (*read)(Aggr*, uint8_t*, Value*);
	void  (*write)(Aggr*, uint8_t*, Value*);
	void  (*merge)(Aggr*, uint8_t*, uint8_t*);
};

struct Aggr
{
	AggrIf* iface;
	List    link;
};

static inline Aggr*
aggr_create(AggrIf* iface, Value* init)
{
	return iface->create(iface, init);
}

static inline void
aggr_free(Aggr* self)
{
	return self->iface->free(self);
}

static inline int
aggr_state_size(Aggr* self)
{
	return self->iface->state_size(self);
}

static inline void
aggr_state_create(Aggr* self, uint8_t* state)
{
	self->iface->state_create(self, state);
}

static inline void
aggr_state_free(Aggr* self, uint8_t* state)
{
	self->iface->state_free(self, state);
}

static inline void
aggr_read(Aggr* self, uint8_t* state, Value* value)
{
	self->iface->read(self, state, value);
}

static inline void
aggr_write(Aggr* self, uint8_t* state, Value* value)
{
	self->iface->write(self, state, value);
}

static inline void
aggr_merge(Aggr* self, uint8_t* state, uint8_t* state_with)
{
	self->iface->merge(self, state, state_with);
}
