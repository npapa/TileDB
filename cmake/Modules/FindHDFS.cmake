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

# Find header files  
if(HDFS_SEARCH_HEADER_PATHS)
  find_path( 
      HDFS_INCLUDE_DIR hdfs.h 
      PATHS ${HDFS_SEARCH_HEADER_PATHS}   
      NO_DEFAULT_PATH
  )
else()
  find_path(HDFS_INCLUDE_DIR hdfs.h)
endif()

# Find library
if(HDFS_SEARCH_LIB_PATH)
  find_library(
      HDFS_LIBRARIES NAMES hdfs
      PATHS ${HDFS_SEARCH_LIB_PATH}$
      NO_DEFAULT_PATH
  )
else()
  find_library(HDFS_LIBRARIES NAMES hdfs)
endif()

if(HDFS_INCLUDE_DIR AND HDFS_LIBRARIES)
  message(STATUS "Found HDFS: ${HDFS_LIBRARIES}")
  set(HDFS_FOUND TRUE)
else()
  set(HDFS_FOUND FALSE)
endif()

if(HDFS_FIND_REQUIRED AND NOT HDFS_FOUND)
  message(FATAL_ERROR "Could not find the HDFS native library.")
endif()