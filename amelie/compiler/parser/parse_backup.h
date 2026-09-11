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

typedef struct AstBackup AstBackup;

struct AstBackup
{
	Ast ast;
};

static inline AstBackup*
ast_backup_of(Ast* ast)
{
	return (AstBackup*)ast;
}

static inline AstBackup*
ast_backup_allocate(void)
{
	AstBackup* self = ast_allocate(0, sizeof(AstBackup));
	return self;
}

void parse_backup(Stmt*);
