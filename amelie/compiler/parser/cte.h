#pragma once

//
// amelie.
//
// Real-Time SQL Database.
//

typedef struct Cte     Cte;
typedef struct CteList CteList;

struct Cte
{
	int     id;
	Ast*    name;
	Columns columns;
	int     stmt;
	Cte*    next;
};

struct CteList
{
	Cte* list;
	Cte* list_tail;
	int  list_count;
};

static inline void
cte_list_init(CteList* self)
{
	self->list       = NULL;
	self->list_tail  = NULL;
	self->list_count = 0;
}

static inline void
cte_list_free(CteList* self)
{
	for (auto cte = self->list; cte; cte = cte->next)
		columns_free(&cte->columns);
	cte_list_init(self);
}

static inline void
cte_list_reset(CteList* self)
{
	cte_list_free(self);
}

static inline Cte*
cte_list_add(CteList* self, Ast* name, int stmt)
{
	Cte* cte = palloc(sizeof(Cte));
	cte->id   = self->list_count;
	cte->name = name;
	cte->stmt = stmt;
	cte->next = NULL;
	columns_init(&cte->columns);
	if (self->list == NULL)
		self->list = cte;
	else
		self->list_tail->next = cte;
	self->list_tail = cte;
	self->list_count++;
	return cte;
}

static inline Cte*
cte_list_find(CteList* self, Str* name)
{
	for (auto cte = self->list; cte; cte = cte->next)
	{
		if (cte->name == NULL)
			continue;
		if (str_compare(&cte->name->string, name))
			return cte;
	}
	return NULL;
}
