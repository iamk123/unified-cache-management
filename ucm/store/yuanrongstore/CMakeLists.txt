find_package(Python COMPONENTS Interpreter QUIET)

if(NOT YUANRONG_SDK_ROOT AND Python_EXECUTABLE)
    execute_process(
        COMMAND ${Python_EXECUTABLE} -c
                "import pathlib, yr.datasystem as ds; print(pathlib.Path(ds.__file__).resolve().parent)"
        OUTPUT_VARIABLE YUANRONG_SDK_ROOT
        OUTPUT_STRIP_TRAILING_WHITESPACE
        ERROR_QUIET
    )
endif()

if(NOT YUANRONG_INCLUDE_DIR AND YUANRONG_SDK_ROOT)
    set(YUANRONG_INCLUDE_DIR "${YUANRONG_SDK_ROOT}/include")
endif()

set(YUANRONG_LIB_DIR "${YUANRONG_SDK_ROOT}/lib")
set(YUANRONG_LIBRARY "${YUANRONG_LIB_DIR}/libdatasystem.so")

if(EXISTS "${YUANRONG_INCLUDE_DIR}/datasystem/hetero_client.h"
   AND EXISTS "${YUANRONG_LIBRARY}"
   AND TARGET Ascend::ascendcl)
    file(GLOB_RECURSE SOURCES "./cc/*.cc")
    add_library(yuanrongstore SHARED ${SOURCES})
    target_compile_features(yuanrongstore PUBLIC cxx_std_20)
    target_include_directories(yuanrongstore PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/cc
        ${YUANRONG_INCLUDE_DIR}
    )
    target_link_libraries(yuanrongstore PUBLIC
        storeintf infra_logger fmt trans ${YUANRONG_LIBRARY} Ascend::ascendcl
    )
    set_target_properties(yuanrongstore PROPERTIES
        BUILD_RPATH "${YUANRONG_LIB_DIR}"
        INSTALL_RPATH "${YUANRONG_LIB_DIR}"
    )
    file(RELATIVE_PATH INSTALL_REL_PATH ${UCM_ROOT_DIR} ${CMAKE_CURRENT_SOURCE_DIR})
    install(TARGETS yuanrongstore LIBRARY DESTINATION ${INSTALL_REL_PATH} COMPONENT ucm)
else()
    message(STATUS "yuanrongstore: skipping build - Yuanrong wheel or Ascend runtime not found")
    message(STATUS "  YUANRONG_SDK_ROOT=${YUANRONG_SDK_ROOT}")
    message(STATUS "  YUANRONG_INCLUDE_DIR=${YUANRONG_INCLUDE_DIR}")
    message(STATUS "  YUANRONG_LIBRARY=${YUANRONG_LIBRARY}")
endif()
