cmake_minimum_required(VERSION 3.20)
project(pg_orca LANGUAGES CXX C)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(CMAKE_POSITION_INDEPENDENT_CODE TRUE)

option(PG_ORCA_ENABLE_DAFT
    "Build the optional ORCA/Daft integration (the Rust bridge remains runtime-loaded)"
    OFF)
message(STATUS "PG_ORCA_ENABLE_DAFT: ${PG_ORCA_ENABLE_DAFT}")

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE "Debug")
endif()
message(STATUS "Build type: ${CMAKE_BUILD_TYPE}")

# -------------------------------------------------------------------
# PostgreSQL
# -------------------------------------------------------------------
find_program(_PG_CONFIG_DEFAULT pg_config)
set(PG_CONFIG "${_PG_CONFIG_DEFAULT}"
    CACHE PATH "Path to pg_config")

# execute_process results are not cached across cmake regeneration
# (e.g. when ninja detects CMakeLists.txt changed and re-invokes cmake).
# Strategy: always run pg_config; if it succeeds update the cache so
# changing PG_CONFIG is picked up immediately; if it fails (pg_config
# not on PATH during regeneration) keep the existing cached value.
macro(pg_config_query FLAG VAR DOC)
    execute_process(COMMAND ${PG_CONFIG} ${FLAG}
        OUTPUT_VARIABLE _pgcfg_result OUTPUT_STRIP_TRAILING_WHITESPACE
        ERROR_QUIET RESULT_VARIABLE _pgcfg_rc)
    if(NOT _pgcfg_rc AND _pgcfg_result)
        set(${VAR} "${_pgcfg_result}" CACHE STRING "${DOC}" FORCE)
    elseif(NOT DEFINED ${VAR})
        set(${VAR} "" CACHE STRING "${DOC}")
    endif()
    # else: pg_config failed but cache already has a value — keep it
endmacro()

pg_config_query(--includedir-server PG_SERVER_INCLUDEDIR "PG server include dir")
pg_config_query(--includedir        PG_INCLUDEDIR        "PG include dir")
pg_config_query(--pkglibdir         PG_PKGLIBDIR         "PG pkglibdir")
pg_config_query(--bindir            PG_BINDIR            "PG bindir")
pg_config_query(--sharedir          PG_SHAREDIR          "PG sharedir")
pg_config_query(--ldflags_sl        PG_LDFLAGS_SL        "PG shared-lib ldflags")
pg_config_query(--version           PG_VERSION           "PG version string")

if(NOT PG_PKGLIBDIR)
    message(FATAL_ERROR "pg_config not found or returned empty output. "
        "Set -DPG_CONFIG=/path/to/pg_config explicitly.")
endif()

message(STATUS "PostgreSQL: ${PG_VERSION}")
message(STATUS "  server headers: ${PG_SERVER_INCLUDEDIR}")
message(STATUS "  pkglibdir:      ${PG_PKGLIBDIR}")

set(PG_ORCA_DAFT_BRIDGE_LIBRARY
    "${PG_PKGLIBDIR}/pg_orca/libdaft_pg_bridge.so"
    CACHE FILEPATH "Default absolute path of the optional Daft bridge")
set(PG_ORCA_DAFT_COST_MODEL_VERSION
    "p0-unavailable"
    CACHE STRING "Build identifier exposed by pg_orca.daft_cost_model_version")
set(PG_ORCA_DAFT_BRIDGE_INCLUDE_DIR
    "${CMAKE_SOURCE_DIR}/../daft/src/daft-pg-bridge/include"
    CACHE PATH "Directory containing the canonical daft_pg_bridge.h")

if(PG_ORCA_ENABLE_DAFT AND
   NOT EXISTS "${PG_ORCA_DAFT_BRIDGE_INCLUDE_DIR}/daft_pg_bridge.h")
    message(FATAL_ERROR
        "PG_ORCA_ENABLE_DAFT=ON requires the canonical bridge header at "
        "${PG_ORCA_DAFT_BRIDGE_INCLUDE_DIR}/daft_pg_bridge.h")
endif()

# -------------------------------------------------------------------
# xerces-c (needed by ORCA for DXL XML parsing).
#
# Default on Linux: build the bundled submodule under third_party/xerces-c
# as a static library folded into pg_orca.so. This makes the extension
# self-contained — no runtime libxerces-c.so dependency — which matters
# for the deb/rpm release packages (self-hosted Supabase, distroless
# containers, hosts without a matching xerces-c package).
#
# Default on macOS: use Homebrew's xerces-c. Release packaging is
# Linux-only; for macOS dev builds the system library is easier and
# avoids quirks of the static build on the Apple toolchain.
#
# Override either way with -DPG_ORCA_BUNDLED_XERCES=ON/OFF.
# -------------------------------------------------------------------
if(APPLE)
    set(_pg_orca_bundled_default OFF)
else()
    set(_pg_orca_bundled_default ON)
endif()
option(PG_ORCA_BUNDLED_XERCES
    "Build xerces-c from third_party/ submodule and link statically"
    ${_pg_orca_bundled_default})

if(PG_ORCA_BUNDLED_XERCES)
    set(_xerces_src "${CMAKE_SOURCE_DIR}/third_party/xerces-c")
    if(NOT EXISTS "${_xerces_src}/CMakeLists.txt")
        message(FATAL_ERROR
            "PG_ORCA_BUNDLED_XERCES=ON but ${_xerces_src}/CMakeLists.txt is "
            "missing. Run: git submodule update --init --recursive")
    endif()

    # Force xerces options before adding its subdirectory. FORCE is required
    # because xerces-c declares these with CACHE; without FORCE our values
    # would be ignored on the second cmake run.
    set(BUILD_SHARED_LIBS    OFF       CACHE BOOL   "" FORCE)
    set(network              OFF       CACHE BOOL   "" FORCE)  # no socket-based entity resolver
    set(transcoder           "gnuiconv" CACHE STRING "" FORCE) # use glibc iconv, not ICU
    set(message-loader       "inmemory" CACHE STRING "" FORCE) # no external catalog files
    set(threads              ON        CACHE BOOL   "" FORCE)
    # Suppress xerces install rules; we never install its headers/libs.
    set(CMAKE_SKIP_INSTALL_RULES_BACKUP "${CMAKE_SKIP_INSTALL_RULES}")
    set(CMAKE_SKIP_INSTALL_RULES TRUE)
    add_subdirectory(third_party/xerces-c EXCLUDE_FROM_ALL)
    set(CMAKE_SKIP_INSTALL_RULES "${CMAKE_SKIP_INSTALL_RULES_BACKUP}")

    set_target_properties(xerces-c PROPERTIES
        POSITION_INDEPENDENT_CODE ON
        CXX_VISIBILITY_PRESET     hidden
        VISIBILITY_INLINES_HIDDEN ON)

    # The xerces-c target should propagate its include dirs as PUBLIC, but it doesn't.
    # Explicitly set XERCES_INCLUDE to both source and build directories for bundled builds.
    set(XERCES_LIB     xerces-c)
    set(XERCES_INCLUDE "${CMAKE_SOURCE_DIR}/third_party/xerces-c/src" "${CMAKE_BINARY_DIR}/third_party/xerces-c/src")
    message(STATUS "xerces-c: bundled static build (third_party/xerces-c)")
else()
    # Legacy path: find a system-installed xerces-c.
    find_package(PkgConfig QUIET)
    if(PKG_CONFIG_FOUND)
        pkg_check_modules(XERCES xerces-c)
    endif()

    if(XERCES_FOUND)
        # pkg-config succeeded — prefer absolute paths from XERCES_LINK_LIBRARIES
        # so the linker doesn't need to search for it. On macOS Homebrew,
        # xerces-c lives in /opt/homebrew/opt/xerces-c/lib which is not a
        # default search path, so passing only "-lxerces-c" fails.
        if(XERCES_LINK_LIBRARIES)
            set(XERCES_LIB ${XERCES_LINK_LIBRARIES})
        else()
            set(XERCES_LIB ${XERCES_LIBRARIES})
        endif()
        set(XERCES_INCLUDE ${XERCES_INCLUDE_DIRS})
        message(STATUS "xerces-c (pkg-config): ${XERCES_LIB}")
    else()
        find_library(XERCES_LIB xerces-c
            PATHS
                /opt/homebrew/opt/xerces-c/lib   # Apple Silicon Homebrew
                /usr/local/opt/xerces-c/lib      # Intel Homebrew
                /usr/local/lib                   # Linux generic
                /usr/lib
                /usr/lib/x86_64-linux-gnu        # Debian/Ubuntu amd64
                /usr/lib/aarch64-linux-gnu       # Debian/Ubuntu arm64
        )
        find_path(XERCES_INCLUDE xercesc/util/XercesDefs.hpp
            PATHS
                /opt/homebrew/opt/xerces-c/include
                /usr/local/opt/xerces-c/include
                /usr/local/include
                /usr/include
        )
        if(NOT XERCES_LIB OR NOT XERCES_INCLUDE)
            message(FATAL_ERROR
                "xerces-c not found. Install it (e.g. 'apt install libxerces-c-dev' "
                "or 'brew install xerces-c'), set -DXERCES_LIB= and "
                "-DXERCES_INCLUDE= manually, or use the default "
                "-DPG_ORCA_BUNDLED_XERCES=ON to build from third_party/.")
        endif()
        message(STATUS "xerces-c (manual): ${XERCES_LIB}, headers: ${XERCES_INCLUDE}")
    endif()
endif()

# -------------------------------------------------------------------
# gettext/libintl headers (needed by libintl used in gpos)
# On Linux, libintl.h is part of glibc and always in the standard
# include path. On macOS it is a separate Homebrew formula.
# -------------------------------------------------------------------
set(EXTRA_SYSTEM_INCLUDES "")
if(APPLE)
    foreach(DIR
            /opt/homebrew/opt/gettext/include   # Apple Silicon Homebrew
            /usr/local/opt/gettext/include)     # Intel Homebrew
        if(EXISTS "${DIR}/libintl.h")
            list(APPEND EXTRA_SYSTEM_INCLUDES "${DIR}")
            break()
        endif()
    endforeach()
endif()

# -------------------------------------------------------------------
# ORCA core libraries (libgpos, libnaucrates, libgpopt, libgpdbcost)
# Built as a single object library so all symbols are available to
# the extension shared library without needing a separate .a/.so.
# -------------------------------------------------------------------
file(GLOB_RECURSE ORCA_CORE_SRC CONFIGURE_DEPENDS
    ${CMAKE_SOURCE_DIR}/libgpos/src/*.cpp
    ${CMAKE_SOURCE_DIR}/libnaucrates/src/*.cpp
    ${CMAKE_SOURCE_DIR}/libgpopt/src/*.cpp
    ${CMAKE_SOURCE_DIR}/libgpdbcost/src/*.cpp
)

# The stock cost-model dispatchers live in orca_core and recognize the
# extension-owned Daft physical operator ids.  Keep the small, PostgreSQL-free
# Daft cost implementation in the same object library so core test binaries do
# not acquire unresolved references to the pg_orca module.
list(APPEND ORCA_CORE_SRC
    ${CMAKE_SOURCE_DIR}/gpopt/daft/capability_snapshot.cpp
    ${CMAKE_SOURCE_DIR}/gpopt/daft/cost_model.cpp
    ${CMAKE_SOURCE_DIR}/gpopt/daft/fragment_fusion.cpp
    ${CMAKE_SOURCE_DIR}/gpopt/daft/physical_operators.cpp
    ${CMAKE_SOURCE_DIR}/gpopt/daft/plan_ir.cpp
    ${CMAKE_SOURCE_DIR}/gpopt/daft/plan_validator.cpp
    ${CMAKE_SOURCE_DIR}/gpopt/daft/rule_registry.cpp
)

add_library(orca_core OBJECT ${ORCA_CORE_SRC})

# Pull in xerces-c headers via the target's PUBLIC interface. For the
# bundled build XERCES_LIB is the "xerces-c" target which propagates its
# header search path; for the system build XERCES_LIB is a library path
# with no usage requirements (XERCES_INCLUDE supplies headers separately,
# see target_include_directories below). OBJECT libs accept
# target_link_libraries since CMake 3.12 — no symbol is actually linked,
# but compile-time properties propagate.
target_link_libraries(orca_core PUBLIC ${XERCES_LIB})

target_compile_definitions(orca_core PUBLIC
    $<$<CONFIG:Debug>:GPOS_DEBUG>
    USE_CMAKE
)

target_compile_options(orca_core PUBLIC
    -Wall
    -Wno-unused-parameter
    -Wno-sign-compare
)

target_include_directories(orca_core PUBLIC
    ${CMAKE_SOURCE_DIR}/libgpos/include
    ${CMAKE_SOURCE_DIR}/libnaucrates/include
    ${CMAKE_SOURCE_DIR}/libgpopt/include
    ${CMAKE_SOURCE_DIR}/libgpdbcost/include
    ${CMAKE_SOURCE_DIR}/include
    ${CMAKE_SOURCE_DIR}           # allows "compat/utils/foo.h" style includes
    ${CMAKE_SOURCE_DIR}/compat   # stub headers replacing GPDB-specific includes
    SYSTEM
    ${PG_SERVER_INCLUDEDIR}
    ${PG_INCLUDEDIR}
    ${XERCES_INCLUDE}
    ${EXTRA_SYSTEM_INCLUDES}
)

# -------------------------------------------------------------------
# PG integration layer (gpopt/ directory)
# -------------------------------------------------------------------
file(GLOB_RECURSE GPOPT_SRC CONFIGURE_DEPENDS
    ${CMAKE_SOURCE_DIR}/gpopt/config/*.cpp
    ${CMAKE_SOURCE_DIR}/gpopt/relcache/*.cpp
    ${CMAKE_SOURCE_DIR}/gpopt/translate/*.cpp
    ${CMAKE_SOURCE_DIR}/gpopt/utils/*.cpp
    ${CMAKE_SOURCE_DIR}/gpopt/daft/*.cpp
    ${CMAKE_SOURCE_DIR}/gpopt/CGPOptimizer.cpp
    ${CMAKE_SOURCE_DIR}/gpopt/gpdbwrappers.cpp
)

# cost_model.cpp is linked through orca_core above.
list(REMOVE_ITEM GPOPT_SRC ${CMAKE_SOURCE_DIR}/gpopt/daft/cost_model.cpp)
list(REMOVE_ITEM GPOPT_SRC ${CMAKE_SOURCE_DIR}/gpopt/daft/capability_snapshot.cpp)
list(REMOVE_ITEM GPOPT_SRC ${CMAKE_SOURCE_DIR}/gpopt/daft/fragment_fusion.cpp)
list(REMOVE_ITEM GPOPT_SRC ${CMAKE_SOURCE_DIR}/gpopt/daft/physical_operators.cpp)
list(REMOVE_ITEM GPOPT_SRC ${CMAKE_SOURCE_DIR}/gpopt/daft/plan_ir.cpp)
list(REMOVE_ITEM GPOPT_SRC ${CMAKE_SOURCE_DIR}/gpopt/daft/plan_validator.cpp)
list(REMOVE_ITEM GPOPT_SRC ${CMAKE_SOURCE_DIR}/gpopt/daft/rule_registry.cpp)
# -------------------------------------------------------------------
# pg_orca shared library (the actual PostgreSQL extension)
# -------------------------------------------------------------------
set(PG_ORCA_DAFT_LOADER_SRC "")
if(PG_ORCA_ENABLE_DAFT)
    list(APPEND PG_ORCA_DAFT_LOADER_SRC
        compat/executor/daft_bridge_loader.cpp)
endif()

add_library(pg_orca MODULE
    pg_orca.cpp
    ${GPOPT_SRC}
    $<TARGET_OBJECTS:orca_core>
    compat/utils/walkers.c
    compat/utils/misc.c
    compat/utils/flatten_join_alias_var.c
    compat/utils/remove_redundant_results.cpp
    compat/executor/dyn_scan.c
    compat/executor/daft_exec.cpp
    compat/executor/daft_arrow.cpp
    ${PG_ORCA_DAFT_LOADER_SRC}
)

set_target_properties(pg_orca PROPERTIES
    PREFIX ""       # PostgreSQL extensions have no "lib" prefix
    SUFFIX ".so"    # pg_config expects .so even on macOS
)

# On macOS the extension must reference the postgres binary as the "loader"
if(APPLE)
    set_target_properties(pg_orca PROPERTIES
        LINK_FLAGS "${PG_LDFLAGS_SL} -bundle_loader ${PG_BINDIR}/postgres"
    )
endif()

target_compile_definitions(pg_orca PRIVATE
    $<$<CONFIG:Debug>:GPOS_DEBUG>
    USE_CMAKE
    PG_ORCA_ENABLE_DAFT=$<BOOL:${PG_ORCA_ENABLE_DAFT}>
    PG_ORCA_DAFT_BRIDGE_DEFAULT="${PG_ORCA_DAFT_BRIDGE_LIBRARY}"
    PG_ORCA_DAFT_COST_MODEL_VERSION="${PG_ORCA_DAFT_COST_MODEL_VERSION}"
)

target_compile_options(pg_orca PRIVATE
    -Wall
    -Wno-unused-parameter
    -Wno-sign-compare
    -Wno-ignored-attributes
    -fvisibility=hidden
)

target_include_directories(pg_orca PRIVATE
    ${CMAKE_SOURCE_DIR}/include
    ${CMAKE_SOURCE_DIR}/libgpos/include
    ${CMAKE_SOURCE_DIR}/libnaucrates/include
    ${CMAKE_SOURCE_DIR}/libgpopt/include
    ${CMAKE_SOURCE_DIR}/libgpdbcost/include
    ${CMAKE_SOURCE_DIR}
    ${CMAKE_SOURCE_DIR}/compat
    ${PG_ORCA_DAFT_BRIDGE_INCLUDE_DIR}
    SYSTEM
    ${PG_SERVER_INCLUDEDIR}
    ${PG_INCLUDEDIR}
    ${XERCES_INCLUDE}
    ${EXTRA_SYSTEM_INCLUDES}
)

target_link_libraries(pg_orca PRIVATE ${XERCES_LIB})

if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    target_link_libraries(pg_orca PRIVATE dl)
endif()

# -------------------------------------------------------------------
# Install
# -------------------------------------------------------------------
install(TARGETS pg_orca
    LIBRARY DESTINATION "${PG_PKGLIBDIR}")

# On macOS, PostgreSQL looks for .dylib but we build .so — create a symlink.
if(APPLE)
    install(CODE "
        execute_process(COMMAND ${CMAKE_COMMAND} -E create_symlink
            pg_orca.so
            \"${PG_PKGLIBDIR}/pg_orca.dylib\")
    ")
endif()

install(FILES
    pg_orca.control
    pg_orca--1.0.0.sql
    DESTINATION "${PG_SHAREDIR}/extension")

# A Daft-capable core package intentionally does not own the optional Rust
# Bridge.  Install only the machine-readable split-package/ABI contract; the
# Bridge itself is deployed to an immutable versioned path by operations.
if(PG_ORCA_ENABLE_DAFT)
    install(FILES
        packaging/pg_orca-daft-manifest.json
        DESTINATION "${PG_SHAREDIR}/extension")
endif()

# -------------------------------------------------------------------
# gporca_test — standalone ORCA optimizer test binary
# Supports: -d <file.mdp>  execute a minidump
#           -p             print the resulting DXL plan
#           -i <plan_id>   select a specific plan (enables enumeration)
#           -T <flag>      set a trace flag
# -------------------------------------------------------------------
add_executable(gporca_test
    tools/gporca_test/main.cpp
    tools/gporca_test/pg_cost_stubs.cpp
    $<TARGET_OBJECTS:orca_core>
)

target_compile_definitions(gporca_test PRIVATE
    $<$<CONFIG:Debug>:GPOS_DEBUG>
    USE_CMAKE
)

target_compile_options(gporca_test PRIVATE
    -Wall
    -Wno-unused-parameter
    -Wno-sign-compare
)

target_include_directories(gporca_test PRIVATE
    ${CMAKE_SOURCE_DIR}/libgpos/include
    ${CMAKE_SOURCE_DIR}/libnaucrates/include
    ${CMAKE_SOURCE_DIR}/libgpopt/include
    ${CMAKE_SOURCE_DIR}/libgpdbcost/include
    ${CMAKE_SOURCE_DIR}/include
    ${CMAKE_SOURCE_DIR}
    ${CMAKE_SOURCE_DIR}/compat
    SYSTEM
    ${PG_SERVER_INCLUDEDIR}
    ${PG_INCLUDEDIR}
    ${XERCES_INCLUDE}
    ${EXTRA_SYSTEM_INCLUDES}
)

target_link_libraries(gporca_test PRIVATE ${XERCES_LIB})

if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    target_link_libraries(gporca_test PRIVATE dl)
endif()

# Pure selected-fragment fusion/final-validator contract test.  It deliberately
# links only the two implementation units under test, so it can run without a
# PostgreSQL backend or a live Daft runtime.
add_executable(daft_final_validator_selftest
    test/daft_final_validator_selftest.cpp
    gpopt/daft/fragment_fusion.cpp
    gpopt/daft/plan_validator.cpp
)
target_include_directories(daft_final_validator_selftest PRIVATE
    ${CMAKE_SOURCE_DIR}/include
    ${CMAKE_SOURCE_DIR}/libgpos/include
    ${CMAKE_SOURCE_DIR}/libgpopt/include
    ${PG_SERVER_INCLUDEDIR}
    ${PG_INCLUDEDIR}
)
target_compile_options(daft_final_validator_selftest PRIVATE -Wall)

# -------------------------------------------------------------------
# Packaging (DEB/RPM via CPack). Linux only.
# -------------------------------------------------------------------
if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    include(${CMAKE_SOURCE_DIR}/packaging/CPackConfig.cmake)
endif()
