/**
 * @file   open_array.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017 TileDB, Inc.
 * @copyright Copyright (c) 2016 MIT and Intel Corporation
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
 * This file implements the OpenArray class.
 */

#include "open_array.h"

namespace tiledb {

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

OpenArray::OpenArray() {
  array_metadata_ = nullptr;
  cnt_ = 0;
}

OpenArray::~OpenArray() {
  delete array_metadata_;
}

/* ****************************** */
/*               API              */
/* ****************************** */

const ArrayMetadata* OpenArray::array_metadata() const {
  return array_metadata_;
}

const URI& OpenArray::array_uri() const {
  return array_metadata_->array_uri();
}

uint64_t OpenArray::cnt() const {
  return cnt_;
}

void OpenArray::decr_cnt() {
  --cnt_;
}

void OpenArray::fragment_metadata_add(FragmentMetadata* metadata) {
  fragment_metadata_[metadata->fragment_uri().to_string()] =
      std::pair<FragmentMetadata*, uint64_t>(metadata, 1);
}

FragmentMetadata* OpenArray::fragment_metadata_get(const URI& fragment_uri) {
  auto it = fragment_metadata_.find(fragment_uri.to_string());
  if (it == fragment_metadata_.end())
    return nullptr;

  ++(it->second.second);
  return it->second.first;
}

void OpenArray::fragment_metadata_rm(const URI& fragment_uri) {
  // Find metadata
  auto it = fragment_metadata_.find(fragment_uri.to_string());
  if (it == fragment_metadata_.end())
    return;

  // Decrement counter
  --(it->second.second);

  // Potentially remove metadata from main memory
  // TODO: The following may be left to a cache manager
  if (it->second.second == 0) {
    delete it->second.first;
    fragment_metadata_.erase(it);
  }
}

void OpenArray::incr_cnt() {
  ++cnt_;
}

void OpenArray::mtx_lock() {
  mtx_.lock();
}

void OpenArray::mtx_unlock() {
  mtx_.unlock();
}

void OpenArray::set_array_metadata(const ArrayMetadata* array_metadata) {
  array_metadata_ = array_metadata;
}

/* ****************************** */
/*        PRIVATE METHODS         */
/* ****************************** */

}  // namespace tiledb