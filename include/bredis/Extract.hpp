//
//
// Copyright (c) 2017 Ivan Baidakou (basiliscos) (the dot dmol at gmail dot com)
//
// Distributed under the MIT Software License
//
#pragma once

#include <iterator>
#include <stdint.h>
#include <string>
#include <vector>
#include <variant>

#include <boost/lexical_cast.hpp>

#include "bredis/Result.hpp"

namespace bredis {

namespace extracts {

struct string_t {
    std::string str;
};

struct error_t {
    std::string str;
};

using int_t = int64_t;

struct nil_t {};

// forward declaration
struct array_holder_t;

using extraction_result_t =
    std::variant<int_t, string_t, error_t, nil_t, array_holder_t>;

struct array_holder_t {
    using recursive_array_t = std::vector<extraction_result_t>;
    recursive_array_t elements;
};

} // namespace extracts

struct extractor {

    extracts::extraction_result_t
    operator()(const markers::string_t &value) const {
        extracts::string_t r;
        r.str = std::string(value);
		return r;
    }

    extracts::extraction_result_t
    operator()(const markers::error_t &value) const {
        extracts::error_t r;
        r.str = std::string(value);
        return r;
    }

    extracts::extraction_result_t
    operator()(const markers::int_t &value) const {
        std::string str = std::string(value);
        return extracts::int_t{boost::lexical_cast<extracts::int_t>(str)};
    }

    extracts::extraction_result_t
    operator()(const markers::nil_t &value) const {
        return extracts::nil_t{};
    }

    extracts::extraction_result_t
    operator()(const markers::array_holder_t &value) const {
        extracts::array_holder_t r;
        r.elements.reserve(value.elements.size());
        for (const auto &v : value.elements) {
            r.elements.emplace_back(std::visit(*this, v));
        }
        return r;
    }
};

} // namespace bredis
