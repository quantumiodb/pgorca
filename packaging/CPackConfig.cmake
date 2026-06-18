# CPack configuration for pg_orca
#
# Builds .deb (Debian/Ubuntu) and .rpm (RHEL/Fedora/SUSE) packages from the
# normal CMake install rules. Drive it with:
#
#   cmake -S . -B build-pkg -DCMAKE_BUILD_TYPE=Release \
#         -DPG_CONFIG=/usr/lib/postgresql/18/bin/pg_config
#   cmake --build build-pkg -j
#   cpack -B build-pkg/pkg --config build-pkg/CPackConfig.cmake -G "DEB;RPM"
#
# Or use packaging/build-packages.sh as a thin wrapper.

# -------------------------------------------------------------------
# Release packages must statically bundle xerces-c. Shipping a .deb/.rpm
# that runtime-depends on the distro's libxerces-c is fragile (Ubuntu
# 24.04 renamed the package to libxerces-c3.2t64, RHEL 9 has no current
# build, Supabase / distroless images often lack it entirely). Refuse to
# package if someone has overridden PG_ORCA_BUNDLED_XERCES=OFF.
# -------------------------------------------------------------------
if(NOT PG_ORCA_BUNDLED_XERCES)
    message(FATAL_ERROR
        "Release packaging requires PG_ORCA_BUNDLED_XERCES=ON so xerces-c "
        "is linked statically into pg_orca.so. Reconfigure without "
        "-DPG_ORCA_BUNDLED_XERCES=OFF.")
endif()

# -------------------------------------------------------------------
# Version: parse from pg_orca.control's default_version (single source).
# -------------------------------------------------------------------
file(READ "${CMAKE_SOURCE_DIR}/pg_orca.control" _ctl)
string(REGEX MATCH "default_version[ \t]*=[ \t]*'([^']+)'" _m "${_ctl}")
if(NOT CMAKE_MATCH_1)
    message(FATAL_ERROR "Could not parse default_version from pg_orca.control")
endif()
set(PG_ORCA_VERSION "${CMAKE_MATCH_1}")
message(STATUS "pg_orca package version: ${PG_ORCA_VERSION}")

# -------------------------------------------------------------------
# Detect target PostgreSQL major version (for package name + dep).
# -------------------------------------------------------------------
string(REGEX MATCH "PostgreSQL ([0-9]+)" _pgm "${PG_VERSION}")
if(NOT CMAKE_MATCH_1)
    message(FATAL_ERROR "Could not parse PG major version from '${PG_VERSION}'")
endif()
set(PG_MAJOR "${CMAKE_MATCH_1}")

# -------------------------------------------------------------------
# Common CPack settings.
# -------------------------------------------------------------------
set(CPACK_PACKAGE_NAME           "postgresql-${PG_MAJOR}-pg-orca")
set(CPACK_PACKAGE_VERSION        "${PG_ORCA_VERSION}")
set(CPACK_PACKAGE_VENDOR         "pg_orca contributors")
set(CPACK_PACKAGE_CONTACT        "pg_orca contributors <noreply@example.com>")
set(CPACK_PACKAGE_DESCRIPTION_SUMMARY
    "ORCA query optimizer extension for PostgreSQL ${PG_MAJOR}")
set(CPACK_PACKAGE_DESCRIPTION
    "pg_orca plugs the ORCA query optimizer (originally from Greenplum / \
Apache Cloudberry) into a standard single-node PostgreSQL ${PG_MAJOR} \
instance via the planner_hook interface. ORCA operates on an XML-based \
intermediate representation called DXL; queries it cannot handle fall \
back transparently to the standard PostgreSQL planner.")
set(CPACK_PACKAGE_HOMEPAGE_URL   "https://github.com/apache/cloudberry")
set(CPACK_RESOURCE_FILE_LICENSE  "${CMAKE_SOURCE_DIR}/LICENSE")
set(CPACK_RESOURCE_FILE_README   "${CMAKE_SOURCE_DIR}/README.md")

# Don't bundle a top-level directory inside the .deb/.rpm payload.
set(CPACK_PACKAGE_FILE_NAME
    "${CPACK_PACKAGE_NAME}_${CPACK_PACKAGE_VERSION}_${CMAKE_SYSTEM_PROCESSOR}")
set(CPACK_PACKAGING_INSTALL_PREFIX "/")
set(CPACK_INCLUDE_TOPLEVEL_DIRECTORY OFF)
set(CPACK_COMPONENTS_GROUPING ALL_COMPONENTS_IN_ONE)

# Strip binaries in Release packages.
if(CMAKE_BUILD_TYPE STREQUAL "Release")
    set(CPACK_STRIP_FILES TRUE)
endif()

# -------------------------------------------------------------------
# DEB-specific.
# -------------------------------------------------------------------
set(CPACK_DEBIAN_PACKAGE_MAINTAINER "${CPACK_PACKAGE_CONTACT}")
set(CPACK_DEBIAN_PACKAGE_SECTION    "database")
set(CPACK_DEBIAN_PACKAGE_PRIORITY   "optional")
# Match dpkg arch (amd64/arm64) rather than CMAKE_SYSTEM_PROCESSOR (x86_64/aarch64).
set(CPACK_DEBIAN_PACKAGE_SHLIBDEPS  ON)
# postgresql-NN supplies pkglibdir/sharedir. xerces-c is statically linked
# into pg_orca.so (enforced above), so no runtime libxerces-c is required.
set(CPACK_DEBIAN_PACKAGE_DEPENDS "postgresql-${PG_MAJOR}")
# Override default file name to use dpkg arch.
execute_process(COMMAND dpkg --print-architecture
    OUTPUT_VARIABLE _deb_arch OUTPUT_STRIP_TRAILING_WHITESPACE
    ERROR_QUIET RESULT_VARIABLE _deb_arch_rc)
if(_deb_arch_rc EQUAL 0 AND _deb_arch)
    set(CPACK_DEBIAN_FILE_NAME
        "${CPACK_PACKAGE_NAME}_${CPACK_PACKAGE_VERSION}_${_deb_arch}.deb")
else()
    set(CPACK_DEBIAN_FILE_NAME DEB-DEFAULT)
endif()

# -------------------------------------------------------------------
# RPM-specific.
# -------------------------------------------------------------------
set(CPACK_RPM_PACKAGE_LICENSE     "Apache-2.0")
set(CPACK_RPM_PACKAGE_GROUP       "Applications/Databases")
set(CPACK_RPM_PACKAGE_URL         "${CPACK_PACKAGE_HOMEPAGE_URL}")
set(CPACK_RPM_PACKAGE_VENDOR      "${CPACK_PACKAGE_VENDOR}")
set(CPACK_RPM_PACKAGE_SUMMARY     "${CPACK_PACKAGE_DESCRIPTION_SUMMARY}")
set(CPACK_RPM_PACKAGE_DESCRIPTION "${CPACK_PACKAGE_DESCRIPTION}")
# PGDG postgresqlNN-server only; xerces-c is bundled into pg_orca.so.
set(CPACK_RPM_PACKAGE_REQUIRES "postgresql${PG_MAJOR}-server")
set(CPACK_RPM_PACKAGE_AUTOREQ  ON)
# Don't claim ownership of system dirs we install into.
set(CPACK_RPM_EXCLUDE_FROM_AUTO_FILELIST_ADDITION
    "${PG_PKGLIBDIR};${PG_SHAREDIR};${PG_SHAREDIR}/extension")
set(CPACK_RPM_FILE_NAME RPM-DEFAULT)
set(CPACK_RPM_PACKAGE_RELEASE 1)

include(CPack)
