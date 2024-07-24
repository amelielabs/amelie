#pragma once

//
// amelie.
//
// Real-Time SQL Database.
//

bool table_index_create(Table*, Transaction*, IndexConfig*, bool);
void table_index_drop(Table*, Transaction*, Str*, bool);
void table_index_rename(Table*, Transaction*, Str*, Str*, bool);
