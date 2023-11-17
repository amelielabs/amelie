#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct AggrIf AggrIf;
typedef struct Aggr   Aggr;

struct AggrIf
{
	Aggr* (*create)(AggrIf*);
	void  (*free)(Aggr*);
	void  (*state_create)(Aggr*, uint8_t*);
	int   (*state_size)(Aggr*);
	void  (*process)(Aggr*, uint8_t*, Value*);
	void  (*merge)(Aggr*, uint8_t*, uint8_t*);
	void  (*convert)(Aggr*, uint8_t*, Value*);
};

struct Aggr
{
	AggrIf* iface;
	List    link;
};

static inline Aggr*
aggr_create(AggrIf* iface)
{
	return iface->create(iface);
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
aggr_process(Aggr* self, uint8_t* state, Value* value)
{
	self->iface->process(self, state, value);
}

static inline void
aggr_merge(Aggr* self, uint8_t* state, uint8_t* state_with)
{
	self->iface->merge(self, state, state_with);
}

static inline void
aggr_convert(Aggr* self, uint8_t* state, Value* value)
{
	self->iface->convert(self, state, value);
}
