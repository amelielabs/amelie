
//
// monotone
//	
// SQL OLTP database
//

#include <monotone_runtime.h>
#include <monotone_io.h>
#include <monotone_data.h>
#include <monotone_lib.h>
#include <monotone_config.h>
#include <monotone_auth.h>
#include <monotone_client.h>
#include <monotone_server.h>
#include <monotone_schema.h>
#include <monotone_transaction.h>
#include <monotone_storage.h>
#include <monotone_wal.h>
#include <monotone_db.h>
#include <monotone_value.h>
#include <monotone_aggr.h>
#include <monotone_request.h>
#include <monotone_vm.h>

OpDesc ops[] =
{
	// control
	{ CRET,               "ret"               },
	{ CNOP,               "nop"               },
	{ CJMP,               "jmp"               },
	{ CJTR,               "jtr"               },
	{ CJNTR,              "jntr"              },

	{ CSEND,              "send"              },
	{ CSLEEP,             "sleep"             },

	// stack
	{ CPUSH,              "push"              },
	{ CPOP,               "pop"               },

	// data
	{ CNULL,              "null"              },
	{ CBOOL,              "bool"              },
	{ CINT,               "int"               },
	{ CINT_MIN,           "int_min"           },
	{ CFLOAT,             "float"             },
	{ CSTRING,            "string"            },
	{ CSTRING_MIN,        "string_min"        },
	{ CPARTIAL,           "partial"           },

	// arguments
	{ CARG,               "arg"               },
	{ CARG_NAME,          "arg_name"          },

	// expr
	{ CBOR,               "bor"               },
	{ CBAND,              "band"              },
	{ CBXOR,              "bxor"              },
	{ CSHL,               "shl"               },
	{ CSHR,               "shr"               },

	{ CNOT,               "not"               },
	{ CEQU,               "equ"               },
	{ CNEQU,              "nequ"              },
	{ CGTE,               "gte"               },
	{ CGT,                "gt"                },
	{ CLTE,               "lte"               },
	{ CLT,                "lt"                },

	{ CADD,               "add"               },
	{ CSUB,               "sub"               },
	{ CMUL,               "mul"               },
	{ CDIV,               "div"               },
	{ CMOD,               "mod"               },
	{ CNEG,               "neg"               },
	{ CCAT,               "cat"               },

	{ CIDX,               "idx"               },
	{ CSIZEOF,            "sizeof"            },

	{ CTO_STRING,         "to_string"         },
	{ CTO_JSON,           "to_json"           },

	// object
	{ CMAP,               "map"               },
	{ CARRAY,             "array"             },

	// object ops
	{ CCOL_SET,           "col_set"           },
	{ CCOL_UNSET,         "col_unset"         },

	{ COBJ_SET,           "obj_set"           },
	{ COBJ_UNSET,         "obj_unset"         },
	{ COBJ_HAS,           "obj_has"           },

	// set
	{ CSET,               "set"               },
	{ CSET_ORDERED,       "set_ordered"       },
	{ CSET_SORT,          "set_sort"          },
	{ CSET_ADD,           "set_add"           },
	{ CSET_SEND,          "set_send"          },

	// group
	{ CGROUP,             "group"             },
	{ CGROUP_ADD_AGGR,    "group_add_aggr"    },
	{ CGROUP_ADD,         "group_add"         },
	{ CGROUP_GET,         "group_get"         },
	{ CGROUP_GET_AGGR,    "group_get_aggr"    },

	// counters
	{ CCNTR_INIT,         "cntr_init"         },
	{ CCNTR_GTE,          "cntr_gte"          },
	{ CCNTR_LTE,          "cntr_lte"          },

	// cursor
	{ CCURSOR_OPEN,       "cursor_open"       },
	{ CCURSOR_OPEN_EXPR,  "cursor_open_expr"  },
	{ CCURSOR_CLOSE,      "cursor_close"      },
	{ CCURSOR_NEXT,       "cursor_next"       },
	{ CCURSOR_READ,       "cursor_read"       },
	{ CCURSOR_IDX,        "cursor_idx"        },

	// functions
	{ CCALL,              "call"              },

	// dml
	{ CINSERT,            "insert"            },
	{ CUPDATE,            "update"            },
	{ CDELETE,            "delete"            }
};
