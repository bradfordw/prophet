#include <ocilib.h>
#include "erl_nif.h"

#define BUFF_LEN 4096
#define SIZE_STR 260
#if defined(OCI_CHARSET_WIDE)

  #define print_mt  wprintf
  #define print_dt  wprintf
  #define sprint_mt swprintf
  #define sprint_dt swprintf

#elif defined(OCI_CHARSET_MIXED)

  #define print_mt  printf
  #define print_dt  wprintf
  #define sprint_mt ocisprintf
  #define sprint_dt swprintf

#else

  #define print_mt  printf
  #define print_dt  printf
  #define sprint_mt ocisprintf
  #define sprint_dt ocisprintf

#endif

struct prophet_oci {
  OCI_Connection *cn;
};

typedef struct prophet_oci prophet_oci;
static ErlNifResourceType* prophet_rsrc;

static ERL_NIF_TERM ATOM_STRING;
static ERL_NIF_TERM ATOM_DOUBLE;
static ERL_NIF_TERM ATOM_INTEGER;
static ERL_NIF_TERM ATOM_UINTEGER;
