#pragma once

//
// monotone
//
// SQL OLTP database
//

void update_set(Value*, uint8_t*, Str*, Value*);
void update_set_array(Value*, uint8_t*, int, Str*, Value*);
void update_unset(Value*, uint8_t*, Str*);
void update_unset_array(Value*, uint8_t*, int, Str*);
