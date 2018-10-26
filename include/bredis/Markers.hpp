//
//
// Copyright (c) 2017 Ivan Baidakou (basiliscos) (the dot dmol at gmail dot com)
//
// Distributed under the MIT Software License
//
#pragma once

#include <string_view>
#include <variant>

namespace bredis {

namespace markers {

using string_t = std::string_view;
struct error_t : public string_t {};
struct int_t : public string_t {};
struct nil_t : public string_t {};

template <typename Iterator> struct array_holder_t;

template <typename Iterator>
using redis_result_t =
    std::variant<int_t, string_t, error_t,
                   nil_t, array_holder_t<Iterator>>;

template <typename Iterator> struct array_holder_t {
    std::vector<redis_result_t<Iterator>> elements;
};

} // namespace markers

} // namespace bredis
