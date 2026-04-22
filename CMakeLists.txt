cmake_minimum_required(VERSION 3.20)
project(pg_orca LANGUAGES CXX C)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(CMAKE_POSITION_INDEPENDENT_CODE TRUE)

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

# -------------------------------------------------------------------
# xerces-c (needed by ORCA for DXL XML parsing)
# Prefer pkg-config (works on Linux and Homebrew macOS); fall back to
# manual find_library + find_path for installations without .pc files.
# -------------------------------------------------------------------
find_package(PkgConfig QUIET)
if(PKG_CONFIG_FOUND)
    pkg_check_modules(XERCES xerces-c)
endif()

if(XERCES_FOUND)
    # pkg-config succeeded — use its results directly
    set(XERCES_LIB   ${XERCES_LIBRARIES})
    set(XERCES_INCLUDE ${XERCES_INCLUDE_DIRS})
    message(STATUS "xerces-c (pkg-config): ${XERCES_LIB}")
else()
    # Fall back to manual search
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
            "or 'brew install xerces-c') or set -DXERCES_LIB= and "
            "-DXERCES_INCLUDE= manually.")
    endif()
    message(STATUS "xerces-c (manual): ${XERCES_LIB}, headers: ${XERCES_INCLUDE}")
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

add_library(orca_core OBJECT ${ORCA_CORE_SRC})

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
    ${CMAKE_SOURCE_DIR}/gpopt/CGPOptimizer.cpp
    ${CMAKE_SOURCE_DIR}/gpopt/gpdbwrappers.cpp
)

# -------------------------------------------------------------------
# pg_orca shared library (the actual PostgreSQL extension)
# -------------------------------------------------------------------
add_library(pg_orca MODULE
    pg_orca.cpp
    ${GPOPT_SRC}
    $<TARGET_OBJECTS:orca_core>
    compat/utils/walkers.c
    compat/utils/misc.c
    compat/utils/flatten_join_alias_var.c
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
)

target_compile_options(pg_orca PRIVATE
    -Wall
    -Wno-unused-parameter
    -Wno-sign-compare
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
    SYSTEM
    ${PG_SERVER_INCLUDEDIR}
    ${PG_INCLUDEDIR}
    ${XERCES_INCLUDE}
    ${EXTRA_SYSTEM_INCLUDES}
)

target_link_libraries(pg_orca PRIVATE ${XERCES_LIB})

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
    pg_orca--1.0.sql
    DESTINATION "${PG_SHAREDIR}/extension")
