#pragma once

#include <cstdlib>

namespace mooncake {

// Read during SpDiag auto-registration at process startup. Set the variable
// before launching Mooncake. SpDiag stores at most 63 bytes of the name.
inline const char* SpDiagProgramName(const char* default_name) {
    const char* name = std::getenv("MOONCAKE_SPDIAG_PROGRAM_NAME");
    return (name != nullptr && name[0] != '\0') ? name : default_name;
}

}  // namespace mooncake
