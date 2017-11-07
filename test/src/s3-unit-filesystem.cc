/**
 * @file   opt-unit_hdfs_fisystem.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017 TileDB, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 *
 * @section DESCRIPTION
 *
 * Tests for HDFS API filesystem functions.
 */

#include "catch.hpp"

#include <cassert>
#include <fstream>
#include <iostream>

#include <s3_filesystem.h>
#include <status.h>

using namespace tiledb;

TEST_CASE("Test S3 filesystem", "[s3]") {
  s3::connect();
  int buffer_size = 5 * 1024 * 1024;
  auto write_buffer = new char[buffer_size];
  for (int i = 0; i < buffer_size; i++) {
    write_buffer[i] = 'a' + (i % 26);
  }
  std::string bucket = "bucket";
  Status st = s3::delete_bucket(bucket.c_str());
  st = s3::create_bucket(bucket.c_str());
  st = s3::create_dir(URI("s3://" + bucket + "/tiledb_test_dir1"));
  st = s3::create_dir(URI("s3://" + bucket + "/tiledb_test_dir1/sdf"));
  st = s3::create_dir(URI("s3://" + bucket + "/tiledb_test_dir1/sdf/ee"));
  st = s3::write_to_file(
      URI("s3://" + bucket + "/tiledb_test_dir1/sdf/rrrrr"),
      write_buffer,
      buffer_size);
  st = s3::write_to_file(
      URI("s3://" + bucket + "/tiledb_test_dir1/sdf/rrrrr"),
      write_buffer,
      buffer_size);
  st = s3::write_to_file(
      URI("s3://" + bucket + "/tiledb_test_dir1/sdf/rrrrr"),
      write_buffer,
      buffer_size);
  int buffer_size_small = 1024 * 1024;
  auto write_buffer_small = new char[buffer_size_small];
  for (int i = 0; i < buffer_size_small; i++) {
    write_buffer_small[i] = 'a' + (i % 26);
  }

  st = s3::write_to_file(
      URI("s3://" + bucket + "/tiledb_test_dir1/sdf/rrrrr"),
      write_buffer_small,
      buffer_size_small);

  st = s3::write_to_file(
      URI("s3://" + bucket + "/tiledb_test_dir1/sdf/e1"),
      write_buffer,
      buffer_size);

  st = s3::create_dir(URI("s3://" + bucket + "/tiledb_test_dir1/sdf1"));
  st = s3::write_to_file(
      URI("s3://" + bucket + "/tiledb_test_dir1/sdf1/rr2"),
      write_buffer,
      buffer_size);
  st = s3::write_to_file(
      URI("s3://" + bucket + "/tiledb_test_dir1/sdf1/rr3"),
      write_buffer,
      buffer_size);
  // st =
  // s3::create_dir(URI("http://s3.us-east-1.amazonaws.com/tiledb_test_dir/sdfsd/vdw"));

  std::vector<std::string> paths;
  s3::ls(URI("s3://" + bucket + "/tiledb_test_dir1"), &paths);
  for (auto path : paths) {
    std::cout << "File: " << path << std::endl;
  }
  std::cout << "Next folder" << std::endl;
  std::vector<std::string> paths1;
  s3::ls(URI("s3://" + bucket + "/tiledb_test_dir1/sdf"), &paths1);
  for (auto path : paths1) {
    std::cout << "File: " << path << std::endl;
  }

  st = s3::flush_file(URI("s3://" + bucket + "/tiledb_test_dir1/sdf/e1"));
  
  auto read_buffer = new char[26];
  st = s3::read_from_file(
      URI("s3://" + bucket + "/tiledb_test_dir1/sdf/e1"), 10, read_buffer, 26);

  bool allok = true;
  for (int i = 0; i < 26; i++) {
    std::cout<< read_buffer[i];
//    if (read_buffer[i] != static_cast<char>('a' + i)) {
      
//      break;
//    }
  }
  std::cout<<std::endl;


  s3::remove_path(URI("s3://" + bucket + "/tiledb_test_dir1/sdf1"));
  std::vector<std::string> paths2;
  s3::ls(URI("s3://" + bucket + "/tiledb_test_dir1/sdf1"), &paths2);
  for (auto path : paths2) {
    std::cout << "File: " << path << std::endl;
  }
  uint64_t nbytes = 0;
  s3::file_size(
      URI("s3://" + bucket + "/tiledb_test_dir1/sdf/ee.dir"), &nbytes);
  std::cout << "Size: " << nbytes << std::endl;
  std::cout << s3::is_dir(URI("s3://" + bucket + "/tiledb_test_dir1/sdf/ee"))
            << std::endl;
  std::cout << s3::is_dir(URI("s3://" + bucket + "/tiledb_test_dir1/sdf/ee1"))
            << std::endl;
  //  st = s3::create_dir(URI("s3:///tiledb_test_dir1/et"));

  //  CHECK(st.ok());
  s3::disconnect();
}
