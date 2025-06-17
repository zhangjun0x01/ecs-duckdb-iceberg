# This file is included by DuckDB's build system. It specifies which extension to load

# Extension from this repo
duckdb_extension_load(iceberg
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
    LOAD_TESTS
    LINKED_LIBS "../../vcpkg_installed/wasm32-emscripten/lib/*.a"
)


if (NOT ${EMSCRIPTEN})
duckdb_extension_load(tpch)
duckdb_extension_load(icu)

endif()

duckdb_extension_load(avro
        SOURCE_DIR "/Users/thijs/DuckDBLabs/duckdb_avro"
)

################## AWS
if (NOT MINGW AND NOT ${EMSCRIPTEN})
    duckdb_extension_load(aws
            LOAD_TESTS
            GIT_URL https://github.com/duckdb/duckdb-aws
            GIT_TAG main
    )
endif ()

if (NOT ${EMSCRIPTEN})
duckdb_extension_load(httpfs
        GIT_URL https://github.com/duckdb/duckdb-httpfs
        GIT_TAG 7b09112ad257249130375c0841d962eecb85662e
        INCLUDE_DIR extension/httpfs/include
)
endif ()
