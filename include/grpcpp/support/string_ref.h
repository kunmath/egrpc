// egrpc grpcpp shim — grpc::string_ref (design §4.5): the non-owning string
// view upstream predates std::string_view with, kept so metadata accessors
// have upstream-identical signatures. Header-only.
#pragma once

#include <cstddef>
#include <cstring>
#include <string>

namespace grpc {

class string_ref {
 public:
  string_ref() = default;
  string_ref(const char* s) : data_(s), length_(std::strlen(s)) {}
  string_ref(const char* s, size_t l) : data_(s), length_(l) {}
  string_ref(const std::string& s) : data_(s.data()), length_(s.size()) {}

  const char* data() const { return data_; }
  size_t size() const { return length_; }
  size_t length() const { return length_; }
  bool empty() const { return length_ == 0; }

  const char* begin() const { return data_; }
  const char* end() const { return data_ + length_; }

  int compare(string_ref x) const {
    size_t min_size = length_ < x.length_ ? length_ : x.length_;
    int r = min_size == 0 ? 0 : std::memcmp(data_, x.data_, min_size);
    if (r != 0) return r;
    if (length_ < x.length_) return -1;
    if (length_ > x.length_) return 1;
    return 0;
  }

 private:
  const char* data_ = nullptr;
  size_t length_ = 0;
};

inline bool operator==(string_ref x, string_ref y) { return x.compare(y) == 0; }
inline bool operator!=(string_ref x, string_ref y) { return x.compare(y) != 0; }
// Ordering is what std::multimap keys need.
inline bool operator<(string_ref x, string_ref y) { return x.compare(y) < 0; }
inline bool operator<=(string_ref x, string_ref y) { return x.compare(y) <= 0; }
inline bool operator>(string_ref x, string_ref y) { return x.compare(y) > 0; }
inline bool operator>=(string_ref x, string_ref y) { return x.compare(y) >= 0; }

}  // namespace grpc
