//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// type_id.h
//
// Identification: src/include/type/type_id.h
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once
#include <fmt/core.h>
namespace bustub {
// Every possible SQL type ID
enum TypeId { INVALID = 0, BOOLEAN, TINYINT, SMALLINT, INTEGER, BIGINT, DECIMAL, VARCHAR, TIMESTAMP };

}  // namespace bustub

template <>
struct fmt::formatter<bustub::TypeId> : formatter<string_view> {
  // parse is inherited from formatter<string_view>.
  template <typename FormatContext>
  auto format(bustub::TypeId t, FormatContext &ctx) const {
    string_view name = "unknown";
    switch (t) {
      case bustub::TypeId::BIGINT:
        name = "BIGINT";
        break;
      case bustub::TypeId::INTEGER:
        name = "INTEGER";
        break;
      case bustub::TypeId::BOOLEAN:
        name = "BOOLEAN";
        break;
      case bustub::INVALID:
        name = "INVALID";
        break;
      case bustub::TINYINT:
        name = "TINYINT";
        break;
      case bustub::SMALLINT:
        name = "SMALLINT";
        break;
      case bustub::DECIMAL:
        name = "DECIMAL";
        break;
      case bustub::VARCHAR:
        name = "VARCHAR";
        break;
      case bustub::TIMESTAMP:
        name = "TIMESTAMP";
        break;
    }
    return formatter<string_view>::format(name, ctx);
  }
};
