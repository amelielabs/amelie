#pragma once

//
// sonata.
//
// Real-Time SQL Database.
//

typedef struct AstPathKey AstPathKey;
typedef struct AstPath    AstPath;
typedef struct Path       Path;

enum
{
	// scan operation with or without key
	// gt, gte, lt, lte
	PATH_SCAN,

	// point lookup with all keys set
	PATH_LOOKUP
};

struct AstPathKey
{
	Key* key;
	Ast* start_op;
	Ast* start;
	Ast* stop_op;
	Ast* stop;
};

struct AstPath
{
	Ast         ast;
	int         type;
	int         match;
	int         match_stop;
	AstPathKey* keys;
	AstPath*    list;
	AstPath*    next;
};

struct Path
{
	Keys*       keys;
	Target*     target;
	TargetList* target_list;
};

static inline AstPath*
ast_path_of(Ast* ast)
{
	return (AstPath*)ast;
}

static inline void
path_init(Path*       self,
          TargetList* target_list,
          Target*     target,
          Keys*       keys)
{
	self->keys        = keys;
	self->target      = target;
	self->target_list = target_list;
}

AstPath* path_create(Path*, Ast*);
