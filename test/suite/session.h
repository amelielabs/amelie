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

TestSession*
test_session_new(TestSuite*, TestEnv*, const char*);
void test_session_free(TestSuite*, TestSession*);
void test_session_connect(TestSuite*, TestSession*, const char*, const char*);
void test_session_execute(TestSuite*, TestSession*, const char*);
void test_session_import(TestSuite*, TestSession*, const char*, const char*);
TestSession*
test_session_find(TestSuite*, const char*);
