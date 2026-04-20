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
set(PG_CONFIG "/Users/jianghua/pg-install/bin/pg_config"
    CACHE PATH "Path to pg_config")

execute_process(COMMAND ${PG_CONFIG} --includedir-server
    OUTPUT_VARIABLE PG_SERVER_INCLUDEDIR OUTPUT_STRIP_TRAILING_WHITESPACE)
execute_process(COMMAND ${PG_CONFIG} --includedir
    OUTPUT_VARIABLE PG_INCLUDEDIR OUTPUT_STRIP_TRAILING_WHITESPACE)
execute_process(COMMAND ${PG_CONFIG} --pkglibdir
    OUTPUT_VARIABLE PG_PKGLIBDIR OUTPUT_STRIP_TRAILING_WHITESPACE)
execute_process(COMMAND ${PG_CONFIG} --bindir
    OUTPUT_VARIABLE PG_BINDIR OUTPUT_STRIP_TRAILING_WHITESPACE)
execute_process(COMMAND ${PG_CONFIG} --sharedir
    OUTPUT_VARIABLE PG_SHAREDIR OUTPUT_STRIP_TRAILING_WHITESPACE)
execute_process(COMMAND ${PG_CONFIG} --ldflags_sl
    OUTPUT_VARIABLE PG_LDFLAGS_SL OUTPUT_STRIP_TRAILING_WHITESPACE)
execute_process(COMMAND ${PG_CONFIG} --version
    OUTPUT_VARIABLE PG_VERSION OUTPUT_STRIP_TRAILING_WHITESPACE)

message(STATUS "PostgreSQL: ${PG_VERSION}")
message(STATUS "  server headers: ${PG_SERVER_INCLUDEDIR}")
message(STATUS "  pkglibdir:      ${PG_PKGLIBDIR}")

# -------------------------------------------------------------------
# xerces-c (needed by ORCA for DXL XML parsing)
# -------------------------------------------------------------------
find_library(XERCES_LIB xerces-c
    PATHS /opt/homebrew/opt/xerces-c/lib /usr/local/lib REQUIRED)
set(XERCES_INCLUDE "/opt/homebrew/opt/xerces-c/include")
message(STATUS "xerces-c: ${XERCES_LIB}")

# -------------------------------------------------------------------
# macOS: gettext headers (needed by libintl used in gpos)
# -------------------------------------------------------------------
set(EXTRA_SYSTEM_INCLUDES "")
if(APPLE)
    foreach(DIR /opt/homebrew/opt/gettext/include /usr/local/opt/gettext/include)
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
    GPOS_DEBUG
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
    GPOS_DEBUG
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
