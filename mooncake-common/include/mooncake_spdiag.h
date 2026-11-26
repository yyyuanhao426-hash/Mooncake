#pragma once

#include "spdiag/auto_perf.h"

// max_slots controls shared memory capacity, not the SDK's local shard size.
static_assert(static_cast<uint32_t>(PerfKey::COUNT) <= SpDiag::MAX_PERF_SLOTS,
              "Mooncake perf points exceed the installed SpDiag SDK capacity");
