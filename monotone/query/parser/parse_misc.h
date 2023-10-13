#pragma once

//
// monotone
//
// SQL OLTP database
//

static inline bool
parse_if_not_exists(Stmt* self)
{
	if (! stmt_if(self, KIF))
		return false;
	if (! stmt_if(self, KNOT))
		error("IF <NOT> EXISTS expected");
	if (! stmt_if(self, KEXISTS))
		error("IF NOT <EXISTS> expected");
	return true;
}

static inline bool
parse_if_exists(Stmt* self)
{
	if (! stmt_if(self, KIF))
		return false;
	if (! stmt_if(self, KEXISTS))
		error("IF <EXISTS> expected");
	return true;
}
