// egrpc grpcpp shim — grpc::string_ref (design §4.5): the non-owning string
// view upstream predates std::string_view with, kept so metadata accessors
// have upstream-identical signatures. Header-only.
#pragma once

#include <cstddef>
#include <cstring>
#include <ostream>
#include <string>

namespace grpc {

class string_ref {
 public:
  static constexpr size_t npos = static_cast<size_t>(-1);

  string_ref() = default;
  string_ref(const char* s) : data_(s), length_(std::strlen(s)) {}
  string_ref(const char* s, size_t l) : data_(s), length_(l) {}
  string_ref(const std::string& s) : data_(s.data()), length_(s.size()) {}

  const char* data() const { return data_; }
  size_t size() const { return length_; }
  size_t length() const { return length_; }
  size_t max_size() const { return npos - 1; }
  bool empty() const { return length_ == 0; }

  const char* begin() const { return data_; }
  const char* end() const { return data_ + length_; }
  const char* cbegin() const { return data_; }
  const char* cend() const { return data_ + length_; }

  char operator[](size_t pos) const { return data_[pos]; }
  char front() const { return data_[0]; }
  char back() const { return data_[length_ - 1]; }

  string_ref substr(size_t pos, size_t n = npos) const {
    if (pos > length_) return string_ref("", static_cast<size_t>(0));
    if (n == npos || pos + n > length_) n = length_ - pos;
    return string_ref(data_ + pos, n);
  }

  bool starts_with(string_ref x) const {
    return length_ >= x.length_ && substr(0, x.length_).compare(x) == 0;
  }
  bool ends_with(string_ref x) const {
    return length_ >= x.length_ && substr(length_ - x.length_, x.length_).compare(x) == 0;
  }

  size_t find(string_ref s) const {
    if (s.length_ > length_) return npos;
    for (size_t i = 0; i + s.length_ <= length_; ++i) {
      if (substr(i, s.length_).compare(s) == 0) return i;
    }
    return npos;
  }
  size_t find(char c) const {
    for (size_t i = 0; i < length_; ++i) {
      if (data_[i] == c) return i;
    }
    return npos;
  }

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

inline std::ostream& operator<<(std::ostream& out, const string_ref& string) {
  return out << std::string(string.data(), string.size());
}

}  // namespace grpc
