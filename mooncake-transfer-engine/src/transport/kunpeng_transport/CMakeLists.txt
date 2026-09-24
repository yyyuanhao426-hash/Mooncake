file(GLOB UB_SOURCES "*.cpp" "urma/urma_endpoint.cpp" "ub_allocator.cpp")

include(CheckCXXSourceCompiles)

# Allow the caller to pin the exact liburma.so path; otherwise fall back to the
# system lookup used by the current UB transport build.
set(URMA_LIBRARY "" CACHE FILEPATH "Path to liburma.so for UB transport")
if(NOT URMA_LIBRARY OR URMA_LIBRARY STREQUAL "")
    find_library(URMA_LIBRARY urma PATHS /usr/lib64 /usr/local/lib64 /usr/lib)
endif()
if(URMA_LIBRARY AND NOT EXISTS "${URMA_LIBRARY}")
    message(FATAL_ERROR
            "URMA_LIBRARY is set to '${URMA_LIBRARY}', but the file does not exist.")
endif()

# Build ub_transport with real URMA library if available
# Never include mock_urma_api.cpp in ub_transport library
# For test, we will use mock_urma_api.cpp directly
if (URMA_LIBRARY)
    add_library(ub_transport OBJECT ${UB_SOURCES})
    target_link_libraries(ub_transport
            PUBLIC
            ${URMA_LIBRARY}
    )
    message(STATUS "Using real URMA library: ${URMA_LIBRARY}")
else()
    # If liburma.so not found, we'll need to handle this differently
    # For now, just build without mock_urma_api.cpp
    list(APPEND UB_SOURCES "urma/mock_urma.cpp")
    add_library(ub_transport OBJECT ${UB_SOURCES})
    message(WARNING "Not Found liburma.so building ub_transport with Mock URMA library")
endif()
target_include_directories(ub_transport
        PUBLIC
        ${urma_INCLUDE_DIR}
        PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/../../../../mooncake-integration/store
)
set(_UB_SAVED_CMAKE_REQUIRED_INCLUDES ${CMAKE_REQUIRED_INCLUDES})
set(CMAKE_REQUIRED_INCLUDES ${urma_INCLUDE_DIR})
check_cxx_source_compiles("
#include <urma_api.h>
int main() {
    urma_seg_cfg_t cfg = {};
    cfg.is_gpu_seg = 1;
    return 0;
}
" HAVE_URMA_SEG_CFG_IS_GPU_SEG)
set(CMAKE_REQUIRED_INCLUDES ${_UB_SAVED_CMAKE_REQUIRED_INCLUDES})
unset(_UB_SAVED_CMAKE_REQUIRED_INCLUDES)
if(NOT HAVE_URMA_SEG_CFG_IS_GPU_SEG)
    message(FATAL_ERROR
            "URMA headers lack urma_seg_cfg_t::is_gpu_seg, which is required for "
            "GDR support. Point -DURMA_ROOT at the directory containing urma_api.h "
            "(e.g. <umdk>/src/urma/lib/urma/core/include) and -DUMDK_ROOT at the "
            "directory containing urma_ubagg.h (e.g. "
            "<umdk>/src/urma/lib/urma/bond/include), or install the UMDK SDK "
            "headers to /usr/include/ub.")
endif()
target_compile_definitions(ub_transport
        PRIVATE HAVE_URMA_SEG_CFG_IS_GPU_SEG=1)
target_link_libraries(ub_transport
        PRIVATE
        JsonCpp::JsonCpp
        glog::glog
        pthread
        SpDiag::spdiag_lib
)