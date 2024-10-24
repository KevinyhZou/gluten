/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <memory>

#include "arrow/c/helpers.h"

#pragma once

namespace gluten {
namespace memory {
class GlutenColumnarBatch {
 public:
  GlutenColumnarBatch(int32_t numColumns, int32_t numRows)
      : numColumns(numColumns), numRows(numRows) {}

  virtual ~GlutenColumnarBatch() = default;

  int32_t GetNumColumns() const {
    return numColumns;
  }

  int32_t GetNumRows() const {
    return numRows;
  }

  virtual std::string GetType() = 0;

  virtual std::shared_ptr<ArrowArray> exportArrowArray() = 0;

  virtual std::shared_ptr<ArrowSchema> exportArrowSchema() = 0;

  virtual int64_t getExportNanos() const {
    return exportNanos_;
  };

 private:
  int32_t numColumns;
  int32_t numRows;

 protected:
  int64_t exportNanos_;
};

class GlutenArrowCStructColumnarBatch : public GlutenColumnarBatch {
 public:
  GlutenArrowCStructColumnarBatch(
      std::unique_ptr<ArrowSchema> cSchema,
      std::unique_ptr<ArrowArray> cArray)
      : GlutenColumnarBatch(cArray->n_children, cArray->length),
        cSchema_(std::move(cSchema)),
        cArray_(std::move(cArray)) {}

  ~GlutenArrowCStructColumnarBatch() override {}

  std::string GetType() override {
    return "arrow_array";
  }

  std::shared_ptr<ArrowSchema> exportArrowSchema() override {
    return cSchema_;
  }

  std::shared_ptr<ArrowArray> exportArrowArray() override {
    return cArray_;
  }

 private:
  std::shared_ptr<ArrowSchema> cSchema_;
  std::shared_ptr<ArrowArray> cArray_;
};

} // namespace memory
} // namespace gluten
