#include "hello.h"

int hello() {
#ifdef HELLO_FROM_RULES_CC
    return 42;
#else
    return 1;
#endif
}
