
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

#include <amelie_runtime.h>
#include <amelie_io.h>
#include <amelie_lib.h>

char *linenoise(const char *prompt);
void  linenoiseSetMultiLine(int ml);
int   linenoiseHistorySave(const char *filename);
int   linenoiseHistoryLoad(const char *filename);
int   linenoiseHistoryAdd(const char *line);
void  linenoiseSetEncodingUtf8(void);
void  linenoiseAtExit(void);

bool
cli_is_terminal(void)
{
	return isatty(STDIN_FILENO);
}

void
cli_open(char* path)
{
	linenoiseHistoryLoad(path);
	linenoiseSetMultiLine(true);
	linenoiseSetEncodingUtf8();
}

void
cli_close()
{
	linenoiseAtExit();
}

void
cli_sync(char* path)
{
	linenoiseHistorySave(path);
}

bool
cli(char* prompt, Str* input)
{
	auto line = linenoise(prompt);
	if (! line)
		return false;
	if (cli_is_terminal())
		linenoiseHistoryAdd(line);
	str_set_allocated(input, line, strlen(line));
	return true;
}
