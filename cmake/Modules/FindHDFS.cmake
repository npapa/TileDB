#
# FindHDFS.cmake
#
#
# The MIT License
#
# Copyright (c) 2016 MIT and Intel Corporation
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.
#
# Finds the HDFS native library. This module defines:
#   - HDFS_INCLUDE_DIR, directory containing headers
#   - HDFS_LIBRARIES, the HDFS library path
#   - HDFS_FOUND, whether HDFS has been found

find_package(JNI)

if (NOT HDFS_FOUND)
  SET (libhdfs_libs "libhdfs")

  MESSAGE("-- Searching for libhdfs")
  IF ( ${HADOOP_HOME} "" )
     MESSAGE("---HADOOP_HOME not specified")
  ELSE ()
     LIST (APPEND POSSILE_PATHS
          "${HADOOP_HOME}/src/c++/libhdfs"
          "${HADOOP_HOME}/c++/${hdfsosdir}/lib"
          "${HADOOP_HOME}/lib/native"
          "${HADOOP_HOME}/include"
        )
  ENDIF()

  MESSAGE("--  Exploring these paths to find libhdfs and hdfs.h: ${POSSILE_PATHS}.")

  IF (UNIX)
    IF (${ARCH64BIT} EQUAL 1)
      SET (hdfsosdir "Linux-amd64-64")
    ELSE()
      SET (hdfsosdir "Linux-i386-32")
    ENDIF()
  ELSEIF(WIN32)
    SET (hdfsosdir "lib")
  ELSE()
    SET (hdfsosdir "unknown")
  ENDIF()
  IF (NOT ("${hdfsosdir}" STREQUAL "unknown"))
    FIND_PATH (HDFS_INCLUDE_DIR NAMES hdfs.h PATHS ${POSSILE_PATHS} NO_DEFAULT_PATH)
    FIND_LIBRARY (HDFS_LIBRARIES NAMES ${libhdfs_libs} PATHS ${POSSILE_PATHS} NO_DEFAULT_PATH)
  ENDIF()



    IF (HDFS_FOUND)
         MESSAGE ("---LIBHDFS ${HDFS_LIBRARIES} found.")
    ELSE()
        MESSAGE ("---LIBHDFS library not found.")
    ENDIF()

    IF (HDFS_INCLUDE_DIR)
         MESSAGE ("---LIBHDFS ${HDFS_INCLUDE_DIR} found.")
    ELSE()
        MESSAGE ("---HDFS header not found.")
    ENDIF()

ENDIF()




# Find header files  
#set(HDFS_INCLUDE_DIR /usr/local/hadoop/hadoop-2.7.2/include)
#set(HDFS_LIBRARIES /usr/local/hadoop/hadoop-2.7.2/lib/native/libhdfs.so)
#set(JRE_LIBRARIES /usr/lib/jvm/java-8-oracle/jre/lib/amd64/server/libjvm.so)

#set(HDFS_INCLUDE_DIR HADOOP_HOME/include)
#set(HDFS_LIBRARIES HADOOP_HOME/lib/native/libhdfs.so)
#set(JRE_LIBRARIES JAVA_HOME/lib/amd64/server/libjvm.so)
#set(HDFS_FOUND TRUE)

#if(HDFS_SEARCH_HEADER_PATHS)
#  find_path( 
#      HDFS_INCLUDE_DIR hdfs.h 
#      PATHS ${HADOOP_HOME}/include   
#      NO_DEFAULT_PATH
#  )
#else()
#  find_path(HDFS_INCLUDE_DIR hdfs.h)
#endif()

# Find library
#if(HDFS_SEARCH_LIB_PATH)
#  find_library(
#      HDFS_LIBRARIES NAMES hdfs
#      PATHS ${HADOOP_HOME}/lib/native
#      NO_DEFAULT_PATH
#  )
#else()
#  find_library(HDFS_LIBRARIES NAMES hdfs)
#endif()

#if(HDFS_INCLUDE_DIR AND HDFS_LIBRARIES)
  message(STATUS "Found HDFS: ${HDFS_LIBRARIES}")
#  set(HDFS_FOUND TRUE)
#else()
#  set(HDFS_FOUND FALSE)
#endif()

if(HDFS_FIND_REQUIRED AND NOT HDFS_FOUND)
  message(FATAL_ERROR "Could not find the HDFS native library.")
endif()