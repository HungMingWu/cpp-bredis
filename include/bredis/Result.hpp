//
//
// Copyright (c) 2017 Ivan Baidakou (basiliscos) (the dot dmol at gmail dot com)
//
// Distributed under the MIT Software License
//
#pragma once

#include <algorithm>
#include <cstdint>
#include <functional>
#include <memory>
#include <vector>
#include <variant>

#include <boost/asio/buffers_iterator.hpp>

#include "Error.hpp"
#include "Markers.hpp"

namespace bredis {

template <typename DynamicBuffer> struct to_iterator {
    using iterator_t = boost::asio::buffers_iterator<
        typename DynamicBuffer::const_buffers_type, char>;
};

using protocol_error_t =  boost::system::error_code;

struct not_enough_data_t {};

namespace parsing_policy {
struct drop_result {};
struct keep_result {};
} // namespace parsing_policy

template <typename Policy> struct positive_parse_result_t {
    markers::redis_result_t result;
    size_t consumed;
};

template <>
struct positive_parse_result_t<parsing_policy::drop_result> {
    size_t consumed;
};

template <typename Policy> struct parse_result_mapper {
    using type = positive_parse_result_t<Policy>;
};

template <typename Policy>
using parse_result_mapper_t =
    typename parse_result_mapper<Policy>::type;

template <typename Policy>
using parse_result_t =
    std::variant<not_enough_data_t, parse_result_mapper_t<Policy>,
                   protocol_error_t>;

} // namespace bredis
