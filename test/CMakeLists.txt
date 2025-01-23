<<<<<<< HEAD
#!/usr/bin/env python3
# (C) Copyright 2024 NOAA/NCEP/EMC.

# Download the test data if we don't have it.
# Code borrowed from NCEPLIB-bufr project...
=======
# CMake configuration for automated testing
cmake_minimum_required(VERSION 3.10)

project(obsforge_tests)

# =============================================================
# Function to add tests for satellite observations dynamically
# =============================================================
function(add_obsforge_test_satobs target_prefix obs_type obs_source sensor_name sat_name test_config mpi_count)

    # bufr2netcdf
    if(test_config STREQUAL "bufr2netcdf") 
        set(target_name "${target_prefix}_${obs_type}_${sat_name}_${test_config}")
        set(env_args "")
        set(command_args
            "${CMAKE_BINARY_DIR}/bin/bufr2netcdf.x testdata/gdas.t00z.${obs_source}.tm00.bufr_d
            testinput/bufr_${obs_type}_mapping.yaml
            testrun/${test_config}/gdas.t00z.${obs_source}.${sensor_name}_{splits/satId}.tm00.nc"
            ${test_config}/gdas.t00z.${obs_source}.${sensor_name}_${sat_name}.tm00.nc
        )
        if(mpi_count GREATER 0)
            set(target_name "${target_name}_mpi${mpi_count}")
            # Add the test
            ecbuild_add_test(
                TARGET ${target_name}
                MPI ${mpi_count}
                TYPE SCRIPT
                COMMAND bash
                ARGS ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh netcdf ${command_args}
                ${env_args}
            )
        else()
            # Add the test
            ecbuild_add_test(
                TARGET ${target_name}
                TYPE SCRIPT
                COMMAND bash
                ARGS ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh netcdf ${command_args}
                ${env_args}
            )
        endif()

    # script2netcdf
    elseif(test_config STREQUAL "script2netcdf")
        set(target_name "${target_prefix}_${obs_type}_${sat_name}_${test_config}")
        set(env_args "")
        set(command_args
            "testinput/bufr_${obs_type}.py testdata/gdas.t00z.${obs_source}.tm00.bufr_d
            testinput/bufr_${obs_type}_mapping.yaml
            testrun/${test_config}/gdas.t00z.${obs_source}.${sensor_name}_{splits/satId}.tm00.nc"
            ${test_config}/gdas.t00z.${obs_source}.${sensor_name}_${sat_name}.tm00.nc
        )
        set(env_args
            "ENVIRONMENT PYTHONPATH=${CMAKE_BINARY_DIR}/lib/python${Python3_VERSION_MAJOR}.${Python3_VERSION_MINOR}/site-packages:$ENV{PYTHONPATH}"
        )
        if(mpi_count GREATER 0)
            set(target_name "${target_name}_mpi${mpi_count}")
            # Add the test
            ecbuild_add_test(
                TARGET ${target_name}
                MPI ${mpi_count}
                TYPE SCRIPT
                COMMAND bash
                ARGS ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh netcdf ${command_args}
                ${env_args}
            )
        else()
            # Add the test
            ecbuild_add_test(
                TARGET ${target_name}
                TYPE SCRIPT
                COMMAND bash
                ARGS ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh netcdf ${command_args}
                ${env_args}
            )
        endif()

    # bufr4backend
    elseif(test_config STREQUAL "bufr4backend")
        set(target_name "${target_prefix}_${obs_type}_${sat_name}_${test_config}")
        set(env_args "")
        if(mpi_count GREATER 0)
            set(target_name "${target_name}_mpi${mpi_count}")
            set(command_args
                "${CMAKE_BINARY_DIR}/bin/time_IodaIO.x testinput/bufr_${test_config}_${obs_type}_mpi${mpi_count}.yaml"
                ${test_config}/gdas.t00z.${obs_source}.${sensor_name}_${sat_name}.tm00_mpi${mpi_count}.nc
            )
            # Add the test
            ecbuild_add_test(
                TARGET ${target_name}
                MPI ${mpi_count}
                TYPE SCRIPT
                COMMAND bash
                ARGS ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh netcdf ${command_args}
                ${env_args}
            )
        else()
            set(command_args
                "${CMAKE_BINARY_DIR}/bin/time_IodaIO.x testinput/bufr_${test_config}_${obs_type}.yaml"
                ${test_config}/gdas.t00z.${obs_source}.${sensor_name}_${sat_name}.tm00.nc
            )
            # Add the test
            ecbuild_add_test(
                TARGET ${target_name}
                TYPE SCRIPT
                COMMAND bash
                ARGS ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh netcdf ${command_args}
                ${env_args}
            )
        endif()

    # script4backend
    elseif(test_config STREQUAL "script4backend")
        set(target_name "${target_prefix}_${obs_type}_${sat_name}_${test_config}")
        set(env_args "")
        if(mpi_count GREATER 0)
            set(target_name "${target_name}_mpi${mpi_count}")
            set(command_args
                "${CMAKE_BINARY_DIR}/bin/time_IodaIO.x testinput/bufr_${test_config}_${obs_type}_mpi${mpi_count}.yaml"
                ${test_config}/gdas.t00z.${obs_source}.${sensor_name}_${sat_name}.tm00_mpi${mpi_count}.nc
            )
            # Add the test
            ecbuild_add_test(
                TARGET ${target_name}
                MPI ${mpi_count}
                TYPE SCRIPT
                COMMAND bash
                ARGS ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh netcdf ${command_args}
                ${env_args}
            )
        else()
            set(command_args
                "${CMAKE_BINARY_DIR}/bin/time_IodaIO.x testinput/bufr_${test_config}_${obs_type}.yaml"
                ${test_config}/gdas.t00z.${obs_source}.${sensor_name}_${sat_name}.tm00.nc
            )
            # Add the test
            ecbuild_add_test(
                TARGET ${target_name}
                TYPE SCRIPT
                COMMAND bash
                ARGS ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh netcdf ${command_args}
                ${env_args}
            )
        endif()

    else()
	    message(Status "Unknown test configuration")     
    endif()

endfunction()

# ================================================================================
# Function to add tests for conventional observations from bufr dump dynamically
# ================================================================================
function(add_obsforge_test_convobs target_prefix obs_type obs_source test_config mpi_count)

    # bufr2netcdf
    if(test_config STREQUAL "bufr2netcdf") 
        set(target_name "${target_prefix}_${obs_type}_${test_config}")
        set(env_args "")
        set(command_args
            "${CMAKE_BINARY_DIR}/bin/bufr2netcdf.x testdata/gdas.t00z.${obs_source}.tm00.bufr_d
            testinput/bufr_${obs_type}_mapping.yaml
            testrun/${test_config}/gdas.t00z.${obs_source}.tm00.nc"
            ${test_config}/gdas.t00z.${obs_source}.tm00.nc
        )
        if(mpi_count GREATER 0)
            set(target_name "${target_name}_mpi${mpi_count}")
            # Add the test
            ecbuild_add_test(
                TARGET ${target_name}
                MPI ${mpi_count}
                TYPE SCRIPT
                COMMAND bash
                ARGS ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh netcdf ${command_args}
                ${env_args}
            )
        else()
            # Add the test
            ecbuild_add_test(
                TARGET ${target_name}
                TYPE SCRIPT
                COMMAND bash
                ARGS ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh netcdf ${command_args}
                ${env_args}
            )
        endif()

    # script2netcdf
    elseif(test_config STREQUAL "script2netcdf")
        set(target_name "${target_prefix}_${obs_type}_${test_config}")
        set(env_args "")
        set(command_args
            "testinput/bufr_${obs_type}.py testdata/gdas.t00z.${obs_source}.tm00.bufr_d
            testinput/bufr_${obs_type}_mapping.yaml
            testrun/${test_config}/gdas.t00z.${obs_source}.tm00.nc"
            ${test_config}/gdas.t00z.${obs_source}.tm00.nc
        )
        set(env_args
            "ENVIRONMENT PYTHONPATH=${CMAKE_BINARY_DIR}/lib/python${Python3_VERSION_MAJOR}.${Python3_VERSION_MINOR}/site-packages:$ENV{PYTHONPATH}"
        )
        if(mpi_count GREATER 0)
            set(target_name "${target_name}_mpi${mpi_count}")
            # Add the test
            ecbuild_add_test(
                TARGET ${target_name}
                MPI ${mpi_count}
                TYPE SCRIPT
                COMMAND bash
                ARGS ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh netcdf ${command_args}
                ${env_args}
            )
        else()
            # Add the test
            ecbuild_add_test(
                TARGET ${target_name}
                TYPE SCRIPT
                COMMAND bash
                ARGS ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh netcdf ${command_args}
                ${env_args}
            )
        endif()

    # bufr4backend
    elseif(test_config STREQUAL "bufr4backend")
        set(target_name "${target_prefix}_${obs_type}_${test_config}")
        set(env_args "")
        if(mpi_count GREATER 0)
            set(target_name "${target_name}_mpi${mpi_count}")
            set(command_args
                "${CMAKE_BINARY_DIR}/bin/time_IodaIO.x testinput/bufr_${test_config}_${obs_type}_mpi${mpi_count}.yaml"
                ${test_config}/gdas.t00z.${obs_source}.tm00_mpi${mpi_count}.nc
            )
            # Add the test
            ecbuild_add_test(
                TARGET ${target_name}
                MPI ${mpi_count}
                TYPE SCRIPT
                COMMAND bash
                ARGS ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh netcdf ${command_args}
                ${env_args}
            )
        else()
            set(command_args
                "${CMAKE_BINARY_DIR}/bin/time_IodaIO.x testinput/bufr_${test_config}_${obs_type}.yaml"
                ${test_config}/gdas.t00z.${obs_source}.tm00.nc
            )
            # Add the test
            ecbuild_add_test(
                TARGET ${target_name}
                TYPE SCRIPT
                COMMAND bash
                ARGS ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh netcdf ${command_args}
                ${env_args}
            )
        endif()

    # script4backend
    elseif(test_config STREQUAL "script4backend")
        set(target_name "${target_prefix}_${obs_type}_${test_config}")
        set(env_args "")
        if(mpi_count GREATER 0)
            set(target_name "${target_name}_mpi${mpi_count}")
            set(command_args
                "${CMAKE_BINARY_DIR}/bin/time_IodaIO.x testinput/bufr_${test_config}_${obs_type}_mpi${mpi_count}.yaml"
                ${test_config}/gdas.t00z.${obs_source}.tm00_mpi${mpi_count}.nc
            )
            # Add the test
            ecbuild_add_test(
                TARGET ${target_name}
                MPI ${mpi_count}
                TYPE SCRIPT
                COMMAND bash
                ARGS ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh netcdf ${command_args}
                ${env_args}
            )
        else()
            set(command_args
                "${CMAKE_BINARY_DIR}/bin/time_IodaIO.x testinput/bufr_${test_config}_${obs_type}.yaml"
                ${test_config}/gdas.t00z.${obs_source}.tm00.nc
            )
            # Add the test
            ecbuild_add_test(
                TARGET ${target_name}
                TYPE SCRIPT
                COMMAND bash
                ARGS ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh netcdf ${command_args}
                ${env_args}
            )
        endif()

    else()
	    message(Status "Unknown test configuration")     
    endif()

endfunction()

# ===============================================
# Set test parameters for satellite observations
# ===============================================

set(satobs_types "satwnd_amv_abi" "satwnd_amv_seviri" "satwnd_amv_viirs" "satwnd_amv_leogeo")
set(test_configs "bufr2netcdf" "script2netcdf" "bufr4backend" "script4backend")
set(mpi_counts_convobs "0" "2")
set(mpi_counts_satobs "0" "4")

# Associate nested lists (satellites) with each observation type
set_property(GLOBAL PROPERTY satwnd_amv_abi_satellites "goes-16;goes-17;goes-18")
set_property(GLOBAL PROPERTY satwnd_amv_seviri_satellites "m8;m9;m10;m11")
set_property(GLOBAL PROPERTY satwnd_amv_viirs_satellites "npp;n20;n21")
set_property(GLOBAL PROPERTY satwnd_amv_leogeo_satellites "multi")

# Get the length of the satobs_types list
list(LENGTH satobs_types satobs_types_length)
math(EXPR satobs_types_last_index "${satobs_types_length} - 1")

# ======================================================
# Generate tests for satellite observations dynamically
# ======================================================
foreach(index RANGE 0 ${satobs_types_last_index})
    list(GET satobs_types ${index} satobs_type)
    
    # Extract components from satobs_type
    string(REPLACE "_" ";" satobs_type_parts "${satobs_type}")
    list(GET satobs_type_parts 0 obs_source)    # Extract "satwnd"
    list(GET satobs_type_parts 2 sensor_name)   # Extract "abi"

    # Retrieve the nested list of satellite IDs for the current observation type
    get_property(satellites GLOBAL PROPERTY ${satobs_type}_satellites)
    foreach(sat_name ${satellites})
	foreach(test_config ${test_configs})
	    foreach(mpi_count ${mpi_counts_satobs})
                add_obsforge_test_satobs("test_obsforge_satobs" ${satobs_type} ${obs_source} ${sensor_name} ${sat_name} ${test_config} ${mpi_count})
            endforeach()
        endforeach()
    endforeach()
endforeach()

# ==================================================================
# Set test parameters for conventional observations from bufr dump
# ==================================================================
set(convobs_types "sfcsno")

# Get the length of the convobs_types list
list(LENGTH convobs_types convobs_types_length)
math(EXPR convobs_types_last_index "${convobs_types_length} - 1")

# =========================================================
# Generate tests for conventional observations dynamically
# =========================================================
foreach(index RANGE 0 ${convobs_types_last_index})
    list(GET convobs_types ${index} convobs_type)
    
    foreach(convobs_type ${convobs_types})
        set(obs_source ${convobs_type})
        string(FIND "${convobs_type}" "prepbufr" INDEX)
        if(INDEX EQUAL -1) #did not find prepbufr
            foreach(test_config ${test_configs})
                foreach(mpi_count ${mpi_counts_convobs})
                    add_obsforge_test_convobs("test_obsforge_convobs" ${convobs_type} ${obs_source} ${test_config} ${mpi_count})
                endforeach()
            endforeach()
        else()
            string(FIND "${convobs_type}" "acft" INDEX)
            if(INDEX GREATER -1) #it's acft_profiles
                foreach(mpi_count ${mpi_counts_convobs})
                    add_obsforge_test_convobs("test_obsforge_convobs" ${convobs_type} ${obs_source} ${test_config} ${mpi_count})
                endforeach()
            else() #it's anything else
                string(REPLACE "_" ";" convobs_type_parts "${convobs_types}")
                list(GET convobs_type_parts 0 obs_source)    # Extract "satwnd"
                list(GET convobs_type_parts 1 sensor_name)   # Extract "abi"
                foreach(mpi_count ${mpi_counts_convobs})
                    add_obsforge_test_convobs("test_obsforge_convobs" ${convobs_type} ${obs_source} ${test_config} ${mpi_count})
                endforeach()
            endif()
        endif()
    endforeach()
endforeach()

# =================================================================================
# Create necessary test directories and links to required data and files
# =================================================================================
# testdata   : input bufr files
# testoutput : test references
# testinput  : mapping files, Python scripts, and test configuration YAML files
# testrun    : test output files 
# =================================================================================

# Create combined obs types
set(obs_types "")
set(obs_types ${satobs_types})
list(APPEND obs_types ${convobs_types})

# Create directories and subdirectories
file(MAKE_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/testinput)
file(MAKE_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/testrun)
# Ensure output directories for each configuration
foreach(test_config ${test_configs})
    file(MAKE_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/testrun/${test_config})
endforeach()

# Initialize the test_input_files and test_input_configs list
set(test_input_files "")
set(test_input_configs "")

# List for mapping files and Python scripts (test_input_files)
foreach(obs_type ${obs_types}) 
    list(APPEND test_input_files
        "bufr_${obs_type}_mapping.yaml"
        "bufr_${obs_type}.py"
    )
endforeach()

# List for mapping files and Python scripts (test_input_configs) 
foreach (test_config ${test_configs})
    foreach (obs_type ${satobs_types})
        list(APPEND test_input_configs
            "bufr_${test_config}_${obs_type}.yaml"
        )
        foreach (mpi_count ${mpi_counts_satobs})
            if(mpi_count GREATER 0)
                list(APPEND test_input_configs
                    "bufr_${test_config}_${obs_type}_mpi${mpi_count}.yaml"
                ) 
	    endif() 		 
        endforeach()
    endforeach()
endforeach()

foreach (test_config ${test_configs})
    foreach (obs_type ${convobs_types})
        list(APPEND test_input_configs
            "bufr_${test_config}_${obs_type}.yaml"
        )
        foreach (mpi_count ${mpi_counts_convobs})
            if(mpi_count GREATER 0)
                list(APPEND test_input_configs
                    "bufr_${test_config}_${obs_type}_mpi${mpi_count}.yaml"
                ) 
	    endif() 		 
        endforeach()
    endforeach()
endforeach()

# Create symlinks for input mapping files and Python scripts
foreach(file ${test_input_files})
    execute_process(
        COMMAND ${CMAKE_COMMAND} -E create_symlink
        ${CMAKE_CURRENT_SOURCE_DIR}/../sorc/spoc/dump/mapping/${file}
        ${CMAKE_CURRENT_BINARY_DIR}/testinput/${file}
    )
endforeach()

# Create symlinks for input configurations files
foreach(FILENAME ${test_input_configs})
  execute_process( COMMAND ${CMAKE_COMMAND} -E create_symlink
    ${CMAKE_CURRENT_SOURCE_DIR}/testinput/${FILENAME}
    ${CMAKE_CURRENT_BINARY_DIR}/testinput/${FILENAME} )
endforeach(FILENAME)

# ===================================================================
# Download the test data ./build/obsForge/test/ if we don't have it
# ===================================================================
>>>>>>> main

set(DOWNLOAD_URL "https://ftp.emc.ncep.noaa.gov/static_files/public/obsforge")
set(FILE_COLLECTION "obsforge-0.0.0.tgz")
#set(FILE_COLLECTION "obsforge-0.0.1.tgz")
<<<<<<< HEAD
set(WHOKNOWS "/work2/noaa/da/nesposito/")

## Upload data if it's not there: 

#message(STATUS "NICKE UPLOADING: " ${CMAKE_CURRENT_BINARY_DIR}/${FILE_COLLECTION})
#message(STATUS "NICKE UPLOADING: " ${WHOKNOWS}/${FILE_COLLECTION})
#if(NOT EXISTS " ${WHOKNOWS}/${FILE_COLLECTION}")
#  message(STATUS "NICKE UPLOADING2: " ${WHOKNOWS}/${FILE_COLLECTION})
#  file(UPLOAD
##    /work2/noaa/da/nesposito/obsForge_20241219/build/obsForge/test/${FILE_COLLECTION}
#    /work2/noaa/da/nesposito/${FILE_COLLECTION}
#    ${DOWNLOAD_URL}/${FILE_COLLECTION}
#    SHOW_PROGRESS
#    STATUS upload_status
#  )
##    INACTIVITY_TIMEOUT 30
#endif()


## Get the test data if we don't have it.

=======

# Get the test data if we don't have it.
>>>>>>> main
if(NOT EXISTS "${CMAKE_CURRENT_BINARY_DIR}/${FILE_COLLECTION}")
   message(STATUS "Downloading: " ${CMAKE_CURRENT_BINARY_DIR}/${FILE_COLLECTION})
  file(DOWNLOAD
    ${DOWNLOAD_URL}/${FILE_COLLECTION}
    ${CMAKE_CURRENT_BINARY_DIR}/${FILE_COLLECTION}
    SHOW_PROGRESS
    STATUS download_status
    INACTIVITY_TIMEOUT 30
  )

  list(GET download_status 0 download_status_num)

  if(NOT download_status_num EQUAL 0 OR NOT EXISTS ${CMAKE_CURRENT_BINARY_DIR}/${FILE_COLLECTION})
    # Remove empty file if download doesn't complete
    file(REMOVE ${CMAKE_CURRENT_BINARY_DIR}/${BUFR_TAR})
    message(STATUS "Could not download test files, not building tests")
    return()
  endif()

<<<<<<< HEAD
#>>emily
# file(COPY ${CMAKE_CURRENT_SOURCE_DIR}/${FILE_COLLECTION} DESTINATION ${CMAKE_CURRENT_BINARY_DIR})
#<<emily
=======
# file(COPY ${CMAKE_CURRENT_SOURCE_DIR}/${FILE_COLLECTION} DESTINATION ${CMAKE_CURRENT_BINARY_DIR})
>>>>>>> main

  add_custom_target(get_obsforge_test_data ALL DEPENDS ${CMAKE_CURRENT_BINARY_DIR}/${FILE_COLLECTION})
  add_custom_command(
    TARGET get_obsforge_test_data
    POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E chdir ${CMAKE_CURRENT_BINARY_DIR} tar xzf ${FILE_COLLECTION}
    COMMAND ${CMAKE_COMMAND} -E chdir ${CMAKE_CURRENT_BINARY_DIR} rm -rf testdata
    COMMAND ${CMAKE_COMMAND} -E chdir ${CMAKE_CURRENT_BINARY_DIR} rm -rf testoutput
    COMMAND ${CMAKE_COMMAND} -E chdir ${CMAKE_CURRENT_BINARY_DIR} mv remote_data/testdata testdata
    COMMAND ${CMAKE_COMMAND} -E chdir ${CMAKE_CURRENT_BINARY_DIR} mv remote_data/testoutput testoutput
    COMMAND ${CMAKE_COMMAND} -E chdir ${CMAKE_CURRENT_BINARY_DIR} rm -rf remote_data)
endif()
<<<<<<< HEAD

message("Emily checking test/CMAKE_CURRENT_SOURCE_DIR - ${CMAKE_CURRENT_SOURCE_DIR}")
message("Emily checking test/CMAKE_CURRENT_BINARY_DIR - ${CMAKE_CURRENT_BINARY_DIR}")

# Function definitions

# The list of all test input config files, which need to be symlink from the build folder.
# (These symlink are created at the end of this file, to give CMake functions setting up certain
# tests a chance to append items to this list).

# List of test cases
list( APPEND test_case
  bufr2netcdf
  script2netcdf
  bufr4backend 
  script4backend 
)

# List of test input files 
list( APPEND test_input
  bufr_satwnd_amv_goes_mapping.yaml
  bufr_satwnd_amv_goes.py
  bufr_sfcsno_mapping.yaml
  bufr_sfcsno.py
  bufr_adpsfc_prepbufr_mapping.yaml
  bufr_adpsfc_prepbufr.py
  bufr_sfcshp_prepbufr_mapping.yaml
  bufr_sfcshp_prepbufr.py
  bufr_acft_profiles_prepbufr_mapping.yaml
  bufr_acft_profiles_prepbufr.py
)

# List of test configurations 
list( APPEND test_config
  bufr_bufr4backend_satwnd_amv_goes.yaml
  bufr_bufr4backend_satwnd_amv_goes_mpi4.yaml
  bufr_script4backend_satwnd_amv_goes.yaml
  bufr_script4backend_satwnd_amv_goes_mpi4.yaml
  bufr_bufr4backend_sfcsno.yaml
  bufr_bufr4backend_sfcsno_mpi2.yaml
  bufr_script4backend_sfcsno.yaml
  bufr_script4backend_sfcsno_mpi2.yaml
  bufr_bufr4backend_adpsfc_prepbufr.yaml
  bufr_bufr4backend_adpsfc_prepbufr_mpi4.yaml
  bufr_script4backend_adpsfc_prepbufr.yaml
  bufr_script4backend_adpsfc_prepbufr_mpi4.yaml
  bufr_bufr4backend_sfcshp_prepbufr.yaml
  bufr_bufr4backend_sfcshp_prepbufr_mpi4.yaml
  bufr_script4backend_sfcshp_prepbufr.yaml
  bufr_script4backend_sfcshp_prepbufr_mpi4.yaml
  bufr_bufr4backend_acft_profiles_prepbufr.yaml
  bufr_bufr4backend_acft_profiles_prepbufr_mpi4.yaml
  bufr_script4backend_acft_profiles_prepbufr.yaml
  bufr_script4backend_acft_profiles_prepbufr_mpi4.yaml
)

# create test intput directories and make links to the input files
file(MAKE_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/testinput)
foreach(FILENAME ${test_input})
  execute_process( COMMAND ${CMAKE_COMMAND} -E create_symlink
    ${CMAKE_CURRENT_SOURCE_DIR}/../sorc/spoc/dump/mapping/${FILENAME}
    ${CMAKE_CURRENT_BINARY_DIR}/testinput/${FILENAME} )
endforeach(FILENAME)

foreach(FILENAME ${test_config})
  execute_process( COMMAND ${CMAKE_COMMAND} -E create_symlink
    ${CMAKE_CURRENT_SOURCE_DIR}/testinput/${FILENAME}
    ${CMAKE_CURRENT_BINARY_DIR}/testinput/${FILENAME} )
endforeach(FILENAME)

# create test output directories
foreach(TESTCASE ${test_case})
  file(MAKE_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/testrun/${TESTCASE})
endforeach(TESTCASE)

# =====================================================================
# Tests for sfcsno 
# =====================================================================
# Perform bufr2netcdf tests
ecbuild_add_test( TARGET  test_obsforge_sfcsno_bufr2netcdf
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
                          "${CMAKE_BINARY_DIR}/bin/bufr2netcdf.x testdata/gdas.t00z.sfcsno.tm00.bufr_d
                                                                 testinput/bufr_sfcsno_mapping.yaml
                                                                 testrun/bufr2netcdf/gdas.t00z.sfcsno.tm00.nc"
                          bufr2netcdf/gdas.t00z.sfcsno.tm00.nc)

ecbuild_add_test( TARGET  test_obsforge_sfcsno_bufr2netcdf_mpi2
                  MPI 2 
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
                          "${CMAKE_BINARY_DIR}/bin/bufr2netcdf.x testdata/gdas.t00z.sfcsno.tm00.bufr_d
                                                                 testinput/bufr_sfcsno_mapping.yaml
                                                                 testrun/bufr2netcdf/gdas.t00z.sfcsno.tm00.nc"
                          bufr2netcdf/gdas.t00z.sfcsno.tm00.nc)

# Perform script2netcdf tests
ecbuild_add_test( TARGET  test_obsforge_sfcsno_script2netcdf
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
                          "testinput/bufr_sfcsno.py testdata/gdas.t00z.sfcsno.tm00.bufr_d
                                                             testinput/bufr_sfcsno_mapping.yaml
                                                             testrun/script2netcdf/gdas.t00z.sfcsno.tm00.nc"
                          script2netcdf/gdas.t00z.sfcsno.tm00.nc
		          ENVIRONMENT PYTHONPATH=${CMAKE_BINARY_DIR}/lib/python${Python3_VERSION_MAJOR}.${Python3_VERSION_MINOR}/site-packages:$ENV{PYTHONPATH}) 

ecbuild_add_test( TARGET  test_obsforge_sfcsno_script2netcdf_mpi2
                  MPI 2 
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
                          "testinput/bufr_sfcsno.py testdata/gdas.t00z.sfcsno.tm00.bufr_d
                                                             testinput/bufr_sfcsno_mapping.yaml
                                                             testrun/script2netcdf/gdas.t00z.sfcsno.tm00.nc"
                          script2netcdf/gdas.t00z.sfcsno.tm00.nc
                          ENVIRONMENT PYTHONPATH=${CMAKE_BINARY_DIR}/lib/python${Python3_VERSION_MAJOR}.${Python3_VERSION_MINOR}/site-packages:$ENV{PYTHONPATH})

# Perform bufr4backend tests
ecbuild_add_test( TARGET  test_obsforge_sfcsno_bufr4backend
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
                          "${CMAKE_BINARY_DIR}/bin/time_IodaIO.x testinput/bufr_bufr4backend_sfcsno.yaml"
                          bufr4backend/gdas.t00z.sfcsno.tm00.nc)

ecbuild_add_test( TARGET  test_obsforge_sfcsno_bufr4backend_mpi2
                  MPI 2 
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
                          "${CMAKE_BINARY_DIR}/bin/time_IodaIO.x testinput/bufr_bufr4backend_sfcsno_mpi2.yaml"
                          bufr4backend/gdas.t00z.sfcsno.tm00_mpi2.nc)

# Perform script4backend tests
ecbuild_add_test( TARGET  test_obsforge_sfcsno_script4backend
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
                          "${CMAKE_BINARY_DIR}/bin/time_IodaIO.x testinput/bufr_script4backend_sfcsno.yaml"
                          script4backend/gdas.t00z.sfcsno.tm00.nc)

 ecbuild_add_test( TARGET test_obsforge_sfcsno_script4backend_mpi2
                  MPI 2 
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
                          "${CMAKE_BINARY_DIR}/bin/time_IodaIO.x testinput/bufr_script4backend_sfcsno_mpi2.yaml"
                          script4backend/gdas.t00z.sfcsno.tm00_mpi2.nc)

# =====================================================================
# Tests for satwnd
# =====================================================================
# Perform bufr2netcdf tests
ecbuild_add_test( TARGET  test_obsforge_satwnd_abi_goes-16_bufr2netcdf
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
                          "${CMAKE_BINARY_DIR}/bin/bufr2netcdf.x testdata/gdas.t00z.satwnd.tm00.bufr_d
                                                                 testinput/bufr_satwnd_amv_goes_mapping.yaml
                                                                 testrun/bufr2netcdf/gdas.t00z.satwnd.abi_{splits/satId}.tm00.nc"
                          bufr2netcdf/gdas.t00z.satwnd.abi_goes-16.tm00.nc)

ecbuild_add_test( TARGET  test_obsforge_satwnd_abi_goes-17_bufr2netcdf
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
                          "${CMAKE_BINARY_DIR}/bin/bufr2netcdf.x testdata/gdas.t00z.satwnd.tm00.bufr_d
                                                                 testinput/bufr_satwnd_amv_goes_mapping.yaml
                                                                 testrun/bufr2netcdf/gdas.t00z.satwnd.abi_{splits/satId}.tm00.nc"
                          bufr2netcdf/gdas.t00z.satwnd.abi_goes-17.tm00.nc)

ecbuild_add_test( TARGET  test_obsforge_satwnd_abi_goes-18_bufr2netcdf
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
                          "${CMAKE_BINARY_DIR}/bin/bufr2netcdf.x testdata/gdas.t00z.satwnd.tm00.bufr_d
                                                                 testinput/bufr_satwnd_amv_goes_mapping.yaml
                                                                 testrun/bufr2netcdf/gdas.t00z.satwnd.abi_{splits/satId}.tm00.nc"
                          bufr2netcdf/gdas.t00z.satwnd.abi_goes-18.tm00.nc)


# Perform bufr2netcdf tests with MPI
ecbuild_add_test( TARGET  test_obsforge_satwnd_abi_goes-16_bufr2netdf_mpi4
   	          MPI 4
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
                          "${CMAKE_BINARY_DIR}/bin/bufr2netcdf.x testdata/gdas.t00z.satwnd.tm00.bufr_d
                                                                 testinput/bufr_satwnd_amv_goes_mapping.yaml
                                                                 testrun/bufr2netcdf/gdas.t00z.satwnd.abi_{splits/satId}.tm00.nc"
                          bufr2netcdf/gdas.t00z.satwnd.abi_goes-16.tm00.nc)

ecbuild_add_test( TARGET  test_obsforge_satwnd_abi_goes-17_bufr2netcdf_mpi8
   	          MPI 8
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
                          "${CMAKE_BINARY_DIR}/bin/bufr2netcdf.x testdata/gdas.t00z.satwnd.tm00.bufr_d
                                                                 testinput/bufr_satwnd_amv_goes_mapping.yaml
                                                                 testrun/bufr2netcdf/gdas.t00z.satwnd.abi_{splits/satId}.tm00.nc"
                          bufr2netcdf/gdas.t00z.satwnd.abi_goes-17.tm00.nc)

ecbuild_add_test( TARGET  test_obsforge_satwnd_abi_goes-18_bufr2netcdf_mpi2
   	          MPI 2 
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
                          "${CMAKE_BINARY_DIR}/bin/bufr2netcdf.x testdata/gdas.t00z.satwnd.tm00.bufr_d
                                                                 testinput/bufr_satwnd_amv_goes_mapping.yaml
                                                                 testrun/bufr2netcdf/gdas.t00z.satwnd.abi_{splits/satId}.tm00.nc"
                          bufr2netcdf/gdas.t00z.satwnd.abi_goes-18.tm00.nc)		  


# Perform script2netcdf tests
#if (${BUILD_PYTHON_BINDINGS})
ecbuild_add_test( TARGET  test_obsforge_satwnd_abi_goes-16_script2netcdf
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
                          "testinput/bufr_satwnd_amv_goes.py testdata/gdas.t00z.satwnd.tm00.bufr_d
                                                             testinput/bufr_satwnd_amv_goes_mapping.yaml
                                                             testrun/script2netcdf/gdas.t00z.satwnd.abi_{splits/satId}.tm00.nc"
                          script2netcdf/gdas.t00z.satwnd.abi_goes-16.tm00.nc
		          ENVIRONMENT PYTHONPATH=${CMAKE_BINARY_DIR}/lib/python${Python3_VERSION_MAJOR}.${Python3_VERSION_MINOR}/site-packages:$ENV{PYTHONPATH})		  

ecbuild_add_test( TARGET  test_obsforge_satwnd_abi_goes-17_script2netcdf
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
                          "testinput/bufr_satwnd_amv_goes.py testdata/gdas.t00z.satwnd.tm00.bufr_d
                                                             testinput/bufr_satwnd_amv_goes_mapping.yaml
                                                             testrun/script2netcdf/gdas.t00z.satwnd.abi_{splits/satId}.tm00.nc"
                          script2netcdf/gdas.t00z.satwnd.abi_goes-17.tm00.nc
		          ENVIRONMENT PYTHONPATH=${CMAKE_BINARY_DIR}/lib/python${Python3_VERSION_MAJOR}.${Python3_VERSION_MINOR}/site-packages:$ENV{PYTHONPATH})		  

ecbuild_add_test( TARGET  test_obsforge_satwnd_abi_goes-18_script2netcdf
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
                          "testinput/bufr_satwnd_amv_goes.py testdata/gdas.t00z.satwnd.tm00.bufr_d
                                                             testinput/bufr_satwnd_amv_goes_mapping.yaml
                                                             testrun/script2netcdf/gdas.t00z.satwnd.abi_{splits/satId}.tm00.nc"
                          script2netcdf/gdas.t00z.satwnd.abi_goes-18.tm00.nc
		          ENVIRONMENT PYTHONPATH=${CMAKE_BINARY_DIR}/lib/python${Python3_VERSION_MAJOR}.${Python3_VERSION_MINOR}/site-packages:$ENV{PYTHONPATH})		  

# Perform script2netcdf tests with MPI
ecbuild_add_test( TARGET  test_obsforge_satwnd_abi_goes-16_script2netcdf_mpi4
	          MPI 4
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
                          "testinput/bufr_satwnd_amv_goes.py testdata/gdas.t00z.satwnd.tm00.bufr_d
                                                             testinput/bufr_satwnd_amv_goes_mapping.yaml
                                                             testrun/script2netcdf/gdas.t00z.satwnd.abi_{splits/satId}.tm00.nc"
                          script2netcdf/gdas.t00z.satwnd.abi_goes-16.tm00.nc
                          ENVIRONMENT PYTHONPATH=${CMAKE_BINARY_DIR}/lib/python${Python3_VERSION_MAJOR}.${Python3_VERSION_MINOR}/site-packages:$ENV{PYTHONPATH})

ecbuild_add_test( TARGET  test_obsforge_satwnd_abi_goes-17_script2netcdf_mpi8
	          MPI 4
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
                          "testinput/bufr_satwnd_amv_goes.py testdata/gdas.t00z.satwnd.tm00.bufr_d
                                                             testinput/bufr_satwnd_amv_goes_mapping.yaml
                                                             testrun/script2netcdf/gdas.t00z.satwnd.abi_{splits/satId}.tm00.nc"
                          script2netcdf/gdas.t00z.satwnd.abi_goes-17.tm00.nc
                          ENVIRONMENT PYTHONPATH=${CMAKE_BINARY_DIR}/lib/python${Python3_VERSION_MAJOR}.${Python3_VERSION_MINOR}/site-packages:$ENV{PYTHONPATH})

ecbuild_add_test( TARGET  test_obsforge_satwnd_abi_goes-18_script2netcdf_mpi2
                  MPI 2	
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
                          "testinput/bufr_satwnd_amv_goes.py testdata/gdas.t00z.satwnd.tm00.bufr_d
                                                             testinput/bufr_satwnd_amv_goes_mapping.yaml
                                                             testrun/script2netcdf/gdas.t00z.satwnd.abi_{splits/satId}.tm00.nc"
                          script2netcdf/gdas.t00z.satwnd.abi_goes-18.tm00.nc
                          ENVIRONMENT PYTHONPATH=${CMAKE_BINARY_DIR}/lib/python${Python3_VERSION_MAJOR}.${Python3_VERSION_MINOR}/site-packages:$ENV{PYTHONPATH})

#endif()
# Perform bufr4backend tests
ecbuild_add_test( TARGET  test_obsforge_satwnd_abi_goes-16_bufr4backend
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
			  "${CMAKE_BINARY_DIR}/bin/time_IodaIO.x testinput/bufr_bufr4backend_satwnd_amv_goes.yaml"
                          bufr4backend/gdas.t00z.satwnd.abi_goes-16.tm00.nc)

ecbuild_add_test( TARGET  test_obsforge_satwnd_abi_goes-17_bufr4backend
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
			  "${CMAKE_BINARY_DIR}/bin/time_IodaIO.x testinput/bufr_bufr4backend_satwnd_amv_goes.yaml"
                          bufr4backend/gdas.t00z.satwnd.abi_goes-17.tm00.nc)

ecbuild_add_test( TARGET  test_obsforge_satwnd_abi_goes-18_bufr4backend
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
			  "${CMAKE_BINARY_DIR}/bin/time_IodaIO.x testinput/bufr_bufr4backend_satwnd_amv_goes.yaml"
                          bufr4backend/gdas.t00z.satwnd.abi_goes-18.tm00.nc)


# Perform bufr4backend tests with MPI
ecbuild_add_test( TARGET  test_obsforge_satwnd_abi_goes-16_bufr4backend_mpi4
                  MPI 4	
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
			  "${CMAKE_BINARY_DIR}/bin/time_IodaIO.x testinput/bufr_bufr4backend_satwnd_amv_goes_mpi4.yaml"
                          bufr4backend/gdas.t00z.satwnd.abi_goes-16.tm00_mpi4.nc)

ecbuild_add_test( TARGET  test_obsforge_satwnd_abi_goes-17_bufr4backend_mpi4
                  MPI 4	
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
			  "${CMAKE_BINARY_DIR}/bin/time_IodaIO.x testinput/bufr_bufr4backend_satwnd_amv_goes_mpi4.yaml"
                          bufr4backend/gdas.t00z.satwnd.abi_goes-17.tm00_mpi4.nc)


ecbuild_add_test( TARGET  test_obsforge_satwnd_abi_goes-18_bufr4backend_mpi4
                  MPI 4 	
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
			  "${CMAKE_BINARY_DIR}/bin/time_IodaIO.x testinput/bufr_bufr4backend_satwnd_amv_goes_mpi4.yaml"
                          bufr4backend/gdas.t00z.satwnd.abi_goes-18.tm00_mpi4.nc)


# Perform script4backend tests
ecbuild_add_test( TARGET  test_obsforge_satwnd_abi_goes-16_script4backend
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
                          "${CMAKE_BINARY_DIR}/bin/time_IodaIO.x testinput/bufr_script4backend_satwnd_amv_goes.yaml"
                          script4backend/gdas.t00z.satwnd.abi_goes-16.tm00.nc)

ecbuild_add_test( TARGET  test_obsforge_satwnd_abi_goes-17_script4backend
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
                          "${CMAKE_BINARY_DIR}/bin/time_IodaIO.x testinput/bufr_script4backend_satwnd_amv_goes.yaml"
                          script4backend/gdas.t00z.satwnd.abi_goes-17.tm00.nc)

ecbuild_add_test( TARGET  test_obsforge_satwnd_abi_goes-18_script4backend
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
                          "${CMAKE_BINARY_DIR}/bin/time_IodaIO.x testinput/bufr_script4backend_satwnd_amv_goes.yaml"
                          script4backend/gdas.t00z.satwnd.abi_goes-18.tm00.nc)

# Perform script4backend tests with MPI
ecbuild_add_test( TARGET  test_obsforge_satwnd_abi_goes-16_script4backend_mpi4
                  MPI 4	
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
                          "${CMAKE_BINARY_DIR}/bin/time_IodaIO.x testinput/bufr_script4backend_satwnd_amv_goes_mpi4.yaml"
                          script4backend/gdas.t00z.satwnd.abi_goes-16.tm00_mpi4.nc)

ecbuild_add_test( TARGET  test_obsforge_satwnd_abi_goes-17_script4backend_mpi4
                  MPI 4	
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
                          "${CMAKE_BINARY_DIR}/bin/time_IodaIO.x testinput/bufr_script4backend_satwnd_amv_goes_mpi4.yaml"
                          script4backend/gdas.t00z.satwnd.abi_goes-17.tm00_mpi4.nc)

ecbuild_add_test( TARGET  test_obsforge_satwnd_abi_goes-18_script4backend_mpi4
                  MPI 4	
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
                          "${CMAKE_BINARY_DIR}/bin/time_IodaIO.x testinput/bufr_script4backend_satwnd_amv_goes_mpi4.yaml"
                          script4backend/gdas.t00z.satwnd.abi_goes-18.tm00_mpi4.nc)

# =====================================================================
# Tests for adpsfc prepbufr
# =====================================================================
# Perform bufr2netcdf tests

ecbuild_add_test( TARGET  test_obsforge_adpsfc_prepbufr_bufr2netcdf
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
                          "${CMAKE_BINARY_DIR}/bin/bufr2netcdf.x testdata/gdas.t00z.prepbufr
                                                                 testinput/bufr_adpsfc_prepbufr_mapping.yaml
                                                                 testrun/bufr2netcdf/gdas.t00z.adpsfc_prepbufr.tm00.nc"
                          bufr2netcdf/gdas.t00z.adpsfc_prepbufr.tm00.nc)

ecbuild_add_test( TARGET  test_obsforge_adpsfc_prepbufr_bufr2netcdf_mpi4
                  MPI 4
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
                          "${CMAKE_BINARY_DIR}/bin/bufr2netcdf.x testdata/gdas.t00z.prepbufr
                                                                 testinput/bufr_adpsfc_prepbufr_mapping.yaml
                                                                 testrun/bufr2netcdf/gdas.t00z.adpsfc_prepbufr.tm00_mpi4.nc"
                          bufr2netcdf/gdas.t00z.adpsfc_prepbufr.tm00_mpi4.nc)

# Perform script2netcdf tests
#if (${BUILD_PYTHON_BINDINGS})

ecbuild_add_test( TARGET  test_obsforge_adpsfc_prepbufr_script2netcdf
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
                          "testinput/bufr_adpsfc_prepbufr.py testdata/gdas.t00z.prepbufr
                                                             testinput/bufr_adpsfc_prepbufr_mapping.yaml
                                                             testrun/script2netcdf/gdas.t00z.adpsfc_prepbufr.tm00.nc"
                          script2netcdf/gdas.t00z.adpsfc_prepbufr.tm00.nc
                          ENVIRONMENT PYTHONPATH=${CMAKE_BINARY_DIR}/lib/python${Python3_VERSION_MAJOR}.${Python3_VERSION_MINOR}/site-packages:$ENV{PYTHONPATH})

# Perform script2netcdf tests with mpi
ecbuild_add_test( TARGET  test_obsforge_adpsfc_prepbufr_script2netcdf_mpi4
                  MPI 4
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
                          "testinput/bufr_adpsfc_prepbufr.py testdata/gdas.t00z.prepbufr
                                                             testinput/bufr_adpsfc_prepbufr_mapping.yaml
                                                             testrun/script2netcdf/gdas.t00z.adpsfc_prepbufr.tm00_mpi4.nc"
                          script2netcdf/gdas.t00z.adpsfc_prepbufr.tm00_mpi4.nc
                          ENVIRONMENT PYTHONPATH=${CMAKE_BINARY_DIR}/lib/python${Python3_VERSION_MAJOR}.${Python3_VERSION_MINOR}/site-packages:$ENV{PYTHONPATH})

#endif()
# Perform bufr4backend tests
ecbuild_add_test( TARGET  test_obsforge_adpsfc_prepbufr_bufr4backend
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
                          "${CMAKE_BINARY_DIR}/bin/time_IodaIO.x testinput/bufr_bufr4backend_adpsfc_prepbufr.yaml"
                          bufr4backend/gdas.t00z.adpsfc_prepbufr.tm00.nc)

ecbuild_add_test( TARGET  test_obsforge_adpsfc_prepbufr_bufr4backend_mpi4
                  MPI 4
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
                          "${CMAKE_BINARY_DIR}/bin/time_IodaIO.x testinput/bufr_bufr4backend_adpsfc_prepbufr_mpi4.yaml"
                          bufr4backend/gdas.t00z.adpsfc_prepbufr.tm00_mpi4.nc)


# Perform script4backend tests
ecbuild_add_test( TARGET  test_obsforge_adpsfc_prepbufr_script4backend
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
                          "${CMAKE_BINARY_DIR}/bin/time_IodaIO.x testinput/bufr_script4backend_adpsfc_prepbufr.yaml"
                          script4backend/gdas.t00z.adpsfc_prepbufr.tm00.nc)

ecbuild_add_test( TARGET  test_obsforge_adpsfc_prepbufr_script4backend_mpi4
                  MPI 4
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
                          "${CMAKE_BINARY_DIR}/bin/time_IodaIO.x testinput/bufr_script4backend_adpsfc_prepbufr_mpi4.yaml"
                          script4backend/gdas.t00z.adpsfc_prepbufr.tm00_mpi4.nc)


# =====================================================================
# Tests for sfcshp prepbufr
# =====================================================================
# Perform bufr2netcdf tests

ecbuild_add_test( TARGET  test_obsforge_sfcshp_prepbufr_bufr2netcdf
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
                          "${CMAKE_BINARY_DIR}/bin/bufr2netcdf.x testdata/gdas.t00z.prepbufr
                                                                 testinput/bufr_sfcshp_prepbufr_mapping.yaml
                                                                 testrun/bufr2netcdf/gdas.t00z.sfcshp_prepbufr.tm00.nc"
                          bufr2netcdf/gdas.t00z.sfcshp_prepbufr.tm00.nc)

ecbuild_add_test( TARGET  test_obsforge_sfcshp_prepbufr_bufr2netcdf_mpi4
                  MPI 4
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
                          "${CMAKE_BINARY_DIR}/bin/bufr2netcdf.x testdata/gdas.t00z.sfcshp.tm00.prepbufr
                                                                 testinput/bufr_sfcshp_prepbufr_mapping.yaml
                                                                 testrun/bufr2netcdf/gdas.t00z.sfcshp_prepbufr.tm00_mpi4.nc"
                          bufr2netcdf/gdas.t00z.sfcshp_prepbufr.tm00_mpi4.nc)

# Perform script2netcdf tests
#if (${BUILD_PYTHON_BINDINGS})

ecbuild_add_test( TARGET  test_obsforge_sfcshp_prepbufr_script2netcdf
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
                          "testinput/bufr_sfcshp_prepbufr.py testdata/gdas.t00z.prepbufr
                                                             testinput/bufr_sfcshp_prepbufr_mapping.yaml
                                                             testrun/script2netcdf/gdas.t00z.sfcshp_prepbufr.tm00.nc"
                          script2netcdf/gdas.t00z.sfcshp_prepbufr.tm00.nc
                          ENVIRONMENT PYTHONPATH=${CMAKE_BINARY_DIR}/lib/python${Python3_VERSION_MAJOR}.${Python3_VERSION_MINOR}/site-packages:$ENV{PYTHONPATH})

# Perform script2netcdf tests with mpi
ecbuild_add_test( TARGET  test_obsforge_sfcshp_prepbufr_script2netcdf_mpi4
                  MPI 4
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
                          "testinput/bufr_sfcshp_prepbufr.py testdata/gdas.t00z.prepbufr
                                                             testinput/bufr_sfcshp_prepbufr_mapping.yaml
                                                             testrun/script2netcdf/gdas.t00z.sfcshp_prepbufr.tm00_mpi4.nc"
                          script2netcdf/gdas.t00z.sfcshp_prepbufr.tm00_mpi4.nc
                          ENVIRONMENT PYTHONPATH=${CMAKE_BINARY_DIR}/lib/python${Python3_VERSION_MAJOR}.${Python3_VERSION_MINOR}/site-packages:$ENV{PYTHONPATH})

#endif()
# Perform bufr4backend tests
ecbuild_add_test( TARGET  test_obsforge_sfcshp_prepbufr_bufr4backend
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
                          "${CMAKE_BINARY_DIR}/bin/time_IodaIO.x testinput/bufr_bufr4backend_sfcshp_prepbufr.yaml"
                          bufr4backend/gdas.t00z.sfcshp_prepbufr.tm00.nc)

ecbuild_add_test( TARGET  test_obsforge_sfcshp_prepbufr_bufr4backend_mpi4
                  MPI 4
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
                          "${CMAKE_BINARY_DIR}/bin/time_IodaIO.x testinput/bufr_bufr4backend_sfcshp_prepbufr_mpi4.yaml"
                          bufr4backend/gdas.t00z.sfcshp_prepbufr.tm00_mpi4.nc)


# Perform script4backend tests
ecbuild_add_test( TARGET  test_obsforge_sfcshp_prepbufr_script4backend
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
                          "${CMAKE_BINARY_DIR}/bin/time_IodaIO.x testinput/bufr_script4backend_sfcshp_prepbufr.yaml"
                          script4backend/gdas.t00z.sfcshp_prepbufr.tm00.nc)

ecbuild_add_test( TARGET  test_obsforge_sfcshp_prepbufr_script4backend_mpi4
                  MPI 4
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
                          "${CMAKE_BINARY_DIR}/bin/time_IodaIO.x testinput/bufr_script4backend_sfcshp_prepbufr_mpi4.yaml"
                          script4backend/gdas.t00z.sfcshp_prepbufr.tm00_mpi4.nc)


# =====================================================================
# Tests for acft_profiles prepbufr
# =====================================================================
# Perform bufr2netcdf tests

ecbuild_add_test( TARGET  test_obsforge_acft_profiles_prepbufr_bufr2netcdf
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
                          "${CMAKE_BINARY_DIR}/bin/bufr2netcdf.x testdata/gdas.t00z.prepbufr.acft_profiles
                                                                 testinput/bufr_acft_profiles_prepbufr_mapping.yaml
                                                                 testrun/bufr2netcdf/gdas.t00z.acft_profiles_prepbufr.tm00.nc"
                          bufr2netcdf/gdas.t00z.acft_profiles_prepbufr.tm00.nc)

ecbuild_add_test( TARGET  test_obsforge_acft_profiles_prepbufr_bufr2netcdf_mpi4
                  MPI 4
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
                          "${CMAKE_BINARY_DIR}/bin/bufr2netcdf.x testdata/gdas.t00z.prepbufr.acft_profiles
                                                                 testinput/bufr_acft_profiles_prepbufr_mapping.yaml
                                                                 testrun/bufr2netcdf/gdas.t00z.acft_profiles_prepbufr.tm00_mpi4.nc"
                          bufr2netcdf/gdas.t00z.acft_profiles_prepbufr.tm00_mpi4.nc)

# Perform script2netcdf tests
#if (${BUILD_PYTHON_BINDINGS})

ecbuild_add_test( TARGET  test_obsforge_acft_profiles_prepbufr_script2netcdf
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
                          "testinput/bufr_acft_profiles_prepbufr.py testdata/gdas.t00z.prepbufr.acft_profiles
                                                             testinput/bufr_acft_profiles_prepbufr_mapping.yaml
                                                             testrun/script2netcdf/gdas.t00z.acft_profiles_prepbufr.tm00.nc"
                          script2netcdf/gdas.t00z.acft_profiles_prepbufr.tm00.nc
                          ENVIRONMENT PYTHONPATH=${CMAKE_BINARY_DIR}/lib/python${Python3_VERSION_MAJOR}.${Python3_VERSION_MINOR}/site-packages:$ENV{PYTHONPATH})

# Perform script2netcdf tests with mpi
ecbuild_add_test( TARGET  test_obsforge_acft_profiles_prepbufr_script2netcdf_mpi4
                  MPI 4
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
                          "testinput/bufr_acft_profiles_prepbufr.py testdata/gdas.t00z.prepbufr.acft_profiles
                                                             testinput/bufr_acft_profiles_prepbufr_mapping.yaml
                                                             testrun/script2netcdf/gdas.t00z.acft_profiles_prepbufr.tm00_mpi4.nc"
                          script2netcdf/gdas.t00z.acft_profiles_prepbufr.tm00_mpi4.nc
                          ENVIRONMENT PYTHONPATH=${CMAKE_BINARY_DIR}/lib/python${Python3_VERSION_MAJOR}.${Python3_VERSION_MINOR}/site-packages:$ENV{PYTHONPATH})

#endif()
# Perform bufr4backend tests
ecbuild_add_test( TARGET  test_obsforge_acft_profiles_prepbufr_bufr4backend
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
                          "${CMAKE_BINARY_DIR}/bin/time_IodaIO.x testinput/bufr_bufr4backend_acft_profiles_prepbufr.yaml"
                          bufr4backend/gdas.t00z.acft_profiles_prepbufr.tm00.nc)

ecbuild_add_test( TARGET  test_obsforge_acft_profiles_prepbufr_bufr4backend_mpi4
                  MPI 4
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
                          "${CMAKE_BINARY_DIR}/bin/time_IodaIO.x testinput/bufr_bufr4backend_acft_profiles_prepbufr_mpi4.yaml"
                          bufr4backend/gdas.t00z.acft_profiles_prepbufr.tm00_mpi4.nc)


# Perform script4backend tests
ecbuild_add_test( TARGET  test_obsforge_acft_profiles_prepbufr_script4backend
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
                          "${CMAKE_BINARY_DIR}/bin/time_IodaIO.x testinput/bufr_script4backend_acft_profiles_prepbufr.yaml"
                          script4backend/gdas.t00z.acft_profiles_prepbufr.tm00.nc)

ecbuild_add_test( TARGET  test_obsforge_acft_profiles_prepbufr_script4backend_mpi4
                  MPI 4
                  TYPE    SCRIPT
                  COMMAND bash
                  ARGS    ${CMAKE_BINARY_DIR}/bin/bufr_comp.sh
                          netcdf
                          "${CMAKE_BINARY_DIR}/bin/time_IodaIO.x testinput/bufr_script4backend_acft_profiles_prepbufr_mpi4.yaml"
                          script4backend/gdas.t00z.acft_profiles_prepbufr.tm00_mpi4.nc)
=======
>>>>>>> main
