/**
 * @file   fragment.cc
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
 * This file implements the Fragment class.
 */

#include "fragment.h"
#include "logger.h"

/* ****************************** */
/*             MACROS             */
/* ****************************** */

namespace tiledb {

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

Fragment::Fragment(Query* query)
    : query_(query) {
  read_state_ = nullptr;
  write_state_ = nullptr;
  metadata_ = nullptr;
  consolidation_ = false;
}

Fragment::~Fragment() {
  if (write_state_ != nullptr) {
    delete write_state_;
    delete metadata_;
  }

  delete read_state_;
}

/* ****************************** */
/*               API              */
/* ****************************** */

<<<<<<< HEAD
uri::URI Fragment::attr_uri(int attribute_id) const {
  const Attribute* attr =
      query_->array()->array_schema()->Attributes()[attribute_id];
  return fragment_uri_.join_path(attr->name() + constants::file_suffix);
}

uri::URI Fragment::attr_var_uri(int attribute_id) const {
  const Attribute* attr =
      query_->array()->array_schema()->Attributes()[attribute_id];
  return fragment_uri_.join_path(
      attr->name() + "_var" + constants::file_suffix);
=======
const ArrayMetadata* Fragment::array_metadata() const {
  return query_->array_metadata();
}

URI Fragment::attr_uri(unsigned int attribute_id) const {
  const Attribute* attr = query_->array_metadata()->attribute(attribute_id);
  return fragment_uri_.join_path(attr->name() + constants::file_suffix);
>>>>>>> upstream/dev
}

URI Fragment::attr_var_uri(unsigned int attribute_id) const {
  const Attribute* attr = query_->array_metadata()->attribute(attribute_id);
  return fragment_uri_.join_path(
<<<<<<< HEAD
      std::string(constants::coords) + constants::file_suffix);
}

const Array* Fragment::array() const {
  return query_->array();
=======
      attr->name() + "_var" + constants::file_suffix);
>>>>>>> upstream/dev
}

URI Fragment::coords_uri() const {
  return fragment_uri_.join_path(
      std::string(constants::coords) + constants::file_suffix);
}

bool Fragment::dense() const {
  return dense_;
}

<<<<<<< HEAD
const uri::URI& Fragment::fragment_uri() const {
  return fragment_uri_;
}

ReadState* Fragment::read_state() const {
  return read_state_;
}

size_t Fragment::tile_size(int attribute_id) const {
  // For easy reference
  const ArraySchema* array_schema = query_->array()->array_schema();
  bool var_size = array_schema->var_size(attribute_id);
  uint64_t cell_var_offset_size = constants::cell_var_offset_size;

  int64_t cell_num_per_tile =
      (dense_) ? array_schema->cell_num_per_tile() : array_schema->capacity();

  return (var_size) ? cell_num_per_tile * cell_var_offset_size :
                      cell_num_per_tile * array_schema->cell_size(attribute_id);
}

/* ****************************** */
/*            MUTATORS            */
/* ****************************** */

=======
>>>>>>> upstream/dev
Status Fragment::finalize() {
  if (write_state_ != nullptr) {  // WRITE
    assert(metadata_ != NULL);
    auto storage_manager = query_->storage_manager();
    Status st_ws = write_state_->finalize();
    Status st_bk = storage_manager->store(metadata_);
    Status st_rn;

    // Rename fragment URI if it is not the result of consolidation
    if (!consolidation_) {
      if (storage_manager->is_dir(fragment_uri_)) {
        URI parent = fragment_uri_.parent();
        std::string last = fragment_uri_.last_path_part();
        URI new_fragment_uri = parent.join_path(last.substr(1));
        st_rn = storage_manager->move_path(fragment_uri_, new_fragment_uri);
      }
    }

    // Errors
    if (!st_ws.ok())
      return st_ws;
    if (!st_bk.ok())
      return st_bk;
    if (!st_rn.ok())
      return st_rn;
  }

  // READ - nothing to be done
  return Status::Ok();
}

const URI& Fragment::fragment_uri() const {
  return fragment_uri_;
}

Status Fragment::init(
    const URI& uri, const void* subarray, bool consolidation) {
  // Set fragment name and consolidation
  fragment_uri_ = uri;
  consolidation_ = consolidation;

  // Check if the fragment is dense or not
  dense_ = true;
  const std::vector<unsigned int>& attribute_ids = query_->attribute_ids();
  auto id_num = (unsigned int)attribute_ids.size();
  unsigned int attribute_num = query_->array_metadata()->attribute_num();
  for (unsigned int i = 0; i < id_num; ++i) {
    if (attribute_ids[i] == attribute_num) {
      dense_ = false;
      break;
    }
  }

  // Initialize metadata and read/write state
  metadata_ = new FragmentMetadata(query_->array_metadata(), dense_, uri);
  read_state_ = nullptr;
  Status st = metadata_->init(subarray);
  if (!st.ok()) {
    delete metadata_;
    metadata_ = nullptr;
    write_state_ = nullptr;
    return st;
  }
  write_state_ = new WriteState(this);

  // Success
  return Status::Ok();
}

Status Fragment::init(const URI& uri, FragmentMetadata* metadata) {
  // Set member attributes
  fragment_uri_ = uri;
  metadata_ = metadata;
  dense_ = metadata_->dense();

  read_state_ = new ReadState(this, query_, metadata_);

  // Success
  return Status::Ok();
}

FragmentMetadata* Fragment::metadata() const {
  return metadata_;
}

Query* Fragment::query() const {
  return query_;
}

ReadState* Fragment::read_state() const {
  return read_state_;
}

uint64_t Fragment::tile_size(unsigned int attribute_id) const {
  // For easy reference
  const ArrayMetadata* array_metadata = query_->array_metadata();
  bool var_size = array_metadata->var_size(attribute_id);
  uint64_t cell_var_offset_size = constants::cell_var_offset_size;

  uint64_t cell_num_per_tile =
      (dense_) ? array_metadata->domain()->cell_num_per_tile() :
                 array_metadata->capacity();

  return (var_size) ?
             cell_num_per_tile * cell_var_offset_size :
             cell_num_per_tile * array_metadata->cell_size(attribute_id);
}

Status Fragment::write(void** buffers, uint64_t* buffer_sizes) {
  // Forward the write command to the write state
  RETURN_NOT_OK(write_state_->write(buffers, buffer_sizes));
  return Status::Ok();
}

}  // namespace tiledb
