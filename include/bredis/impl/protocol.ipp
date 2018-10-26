//
//
// Copyright (c) 2017 Ivan Baidakou (basiliscos) (the dot dmol at gmail dot com)
//
// Distributed under the MIT Software License
//
#pragma once

#include <algorithm>
#include <boost/asio/buffers_iterator.hpp>
#include <boost/lexical_cast.hpp>
#include <variant>
#include <errno.h>
#include <stdlib.h>
#include <string>

namespace bredis {

struct static_string_t {
    const char *begin;
    size_t size;

    constexpr static_string_t(const char *ptr, size_t sz)
        : begin{ptr}, size{sz} {}

    template <typename Iterator>
    Iterator search(Iterator first, Iterator last) const {
        auto *end = begin + size;
        for (;; ++first) {
            Iterator it = first;
            for (auto *s_it = begin;; ++it, ++s_it) {
                if (s_it == end) {
                    return first;
                }
                if (it == last) {
                    return last;
                }
                if (!(*it == *s_it)) {
                    break;
                }
            }
        }
    }

    template <typename Iterator>
    bool equal(Iterator first, Iterator last) const {
        auto *start = begin;
        auto *end = begin + size;

        for (; (first != last) && (start != end); ++first, ++start) {
            if (!(*first == *start)) {
                return false;
            }
        }
        return true;
    }
};

template <typename T> T &operator<<(T &stream, const static_string_t &str) {
    return stream << str.begin;
}

namespace {
constexpr static_string_t terminator{"\r\n", 2};
}

namespace details {

// forward declaration
template <typename Iterator, typename Policy>
parse_result_t<Iterator, Policy> raw_parse(const Iterator &from,
                                           const Iterator &to);

struct count_value_t {
    size_t value;
    size_t consumed;
};

template <typename Iterator, typename Policy>
using count_variant_t =
    std::variant<count_value_t, parse_result_t<Iterator, Policy>>;

template <typename Iterator, typename Policy> struct markup_helper_t {
    using result_wrapper_t = parse_result_t<Iterator, Policy>;
    using positive_wrapper_t = parse_result_mapper_t<Iterator, Policy>;
    using result_t = markers::redis_result_t<Iterator>;
    using string_t = markers::string_t<Iterator>;

    static auto markup_string(size_t consumed, const Iterator &from,
                              const Iterator &to) -> result_wrapper_t {
        return positive_wrapper_t{
            result_t{markers::string_t<Iterator>{from, to}}, consumed};
    }

    static auto markup_nil(size_t consumed, const string_t &str)
        -> result_wrapper_t {
        return positive_wrapper_t{
            result_t{markers::nil_t<Iterator>{str}}, consumed};
    }

    static auto markup_error(positive_wrapper_t &wrapped_string)
        -> result_wrapper_t {
        auto &str = std::get<string_t>(wrapped_string.result);
        return positive_wrapper_t{result_t{markers::error_t<Iterator>{str}},
                               wrapped_string.consumed};
    }

    static auto markup_int(positive_wrapper_t &wrapped_string)
        -> result_wrapper_t {
        auto &str = std::get<string_t>(wrapped_string.result);
        return positive_wrapper_t{
            result_t{markers::int_t<Iterator>{str}}, wrapped_string.consumed};
    }
};

template <typename Iterator>
struct markup_helper_t<Iterator, parsing_policy::drop_result> {
    using policy_t = parsing_policy::drop_result;
    using result_wrapper_t = parse_result_t<Iterator, policy_t>;
    using positive_wrapper_t = parse_result_mapper_t<Iterator, policy_t>;
    using string_t = markers::string_t<Iterator>;

    static auto markup_string(size_t consumed, const Iterator &from,
                              const Iterator &to) -> result_wrapper_t {
        return positive_wrapper_t{consumed};
    }

    static auto markup_nil(size_t consumed, const string_t &str)
        -> result_wrapper_t {
        return positive_wrapper_t{consumed};
    }

    static auto markup_error(positive_wrapper_t &wrapped_string)
        -> result_wrapper_t {
        return positive_wrapper_t{wrapped_string.consumed};
    }

    static auto markup_int(positive_wrapper_t &wrapped_string)
        -> result_wrapper_t {
        return positive_wrapper_t{wrapped_string.consumed};
    }
};

template <typename Iterator, typename Policy> struct array_helper_t {
    using policy_t = Policy;
    using array_t = markers::array_holder_t<Iterator>;
    using item_t = parse_result_mapper_t<Iterator, policy_t>;
    using result_t = parse_result_t<Iterator, policy_t>;

    size_t consumed_;
    size_t count_;
    array_t array_;

    array_helper_t(size_t consumed, size_t count)
        : consumed_{consumed}, count_{count} {
        array_.elements.reserve(count);
    }

    void push(const item_t &item) {
        consumed_ += item.consumed;
        array_.elements.emplace_back(item.result);
    }

    result_t get() { return item_t{std::move(array_), consumed_}; }
};

template <typename Iterator>
struct array_helper_t<Iterator, parsing_policy::drop_result> {
    using policy_t = parsing_policy::drop_result;
    using array_t = markers::array_holder_t<Iterator>;
    using item_t = parse_result_mapper_t<Iterator, policy_t>;
    using result_t = parse_result_t<Iterator, policy_t>;

    size_t consumed_;

    array_helper_t(size_t consumed, size_t count) : consumed_{consumed} {}

    void push(const item_t &item) { consumed_ += item.consumed; }

    result_t get() { return result_t{item_t{consumed_}}; }
};

template <typename Iterator, typename Policy>
struct unwrap_count_t {
    using wrapped_result_t = count_variant_t<Iterator, Policy>;
    using negative_result_t = parse_result_t<Iterator, Policy>;
    using positive_input_t =
        parse_result_mapper_t<Iterator, parsing_policy::keep_result>;

    template <typename T> wrapped_result_t operator()(const T &value) const {
        return negative_result_t{value};
    }

    wrapped_result_t operator()(const positive_input_t &value) const {
        using string_t = markers::string_t<Iterator>;
        using helper = markup_helper_t<Iterator, Policy>;

        auto &count_string_ref = std::get<string_t>(value.result);
        std::string count_string{count_string_ref.from, count_string_ref.to};
        auto count_consumed = value.consumed;
        const char *count_ptr = count_string.c_str();
        char *count_end;

        errno = 0;
        long count = strtol(count_ptr, &count_end, 10);
        if (errno) {
            return protocol_error_t{
                Error::make_error_code(bredis_errors::count_conversion)};
        } else if (count == -1) {
            return helper::markup_nil(count_consumed, count_string_ref);
        } else if (count < -1) {
            return protocol_error_t{
                Error::make_error_code(bredis_errors::count_range)};
        }

        return count_value_t{static_cast<size_t>(count), count_consumed};
    }
};

template <typename Iterator, typename Policy> struct string_parser_t {
    static auto apply(const Iterator &from, const Iterator &to,
                      size_t already_consumed)
        -> parse_result_t<Iterator, Policy> {
        using helper = markup_helper_t<Iterator, Policy>;

        auto found_terminator = terminator.search(from, to);

        if (found_terminator == to) {
            return not_enough_data_t{};
        }

        size_t consumed = already_consumed + terminator.size +
                          std::distance(from, found_terminator);
        return helper::markup_string(consumed, from, found_terminator);
    }
};

template <typename Iterator, typename Policy> struct error_parser_t {
    static auto apply(const Iterator &from, const Iterator &to,
                      size_t already_consumed)
        -> parse_result_t<Iterator, Policy> {
        using helper = markup_helper_t<Iterator, Policy>;
        using parser_t = string_parser_t<Iterator, Policy>;
        using wrapped_result_t = parse_result_mapper_t<Iterator, Policy>;

        auto result = parser_t::apply(from, to, already_consumed);
        if (auto *wrapped_string = std::get_if<wrapped_result_t>(&result); !wrapped_string) {
            return result;
        } else {
	    return helper::markup_error(*wrapped_string);
	}
    }
};

template <typename Iterator, typename Policy> struct int_parser_t {
    static auto apply(const Iterator &from, const Iterator &to,
                      size_t already_consumed)
        -> parse_result_t<Iterator, Policy> {
        using helper = markup_helper_t<Iterator, Policy>;
        using parser_t = string_parser_t<Iterator, Policy>;
        using wrapped_result_t = parse_result_mapper_t<Iterator, Policy>;

        auto result = parser_t::apply(from, to, already_consumed);
        if (auto *wrapped_string = std::get_if<wrapped_result_t>(&result); !wrapped_string) {
            return result;
        } else {
            return helper::markup_int(*wrapped_string);
	}
    }
};

template <typename Iterator, typename Policy> struct bulk_string_parser_t {
    static auto apply(const Iterator &from, const Iterator &to,
                      size_t already_consumed)
        -> parse_result_t<Iterator, Policy> {

        using helper = markup_helper_t<Iterator, Policy>;
        using count_unwrapper_t = unwrap_count_t<Iterator, Policy>;
        using keep_policy = parsing_policy::keep_result;
        using count_parser_t = string_parser_t<Iterator, keep_policy>;
        using result_t = parse_result_t<Iterator, Policy>;

        auto count_result = count_parser_t::apply(from, to, already_consumed);
        auto count_int_result =
            std::visit(count_unwrapper_t{}, count_result);
        if (auto *count_wrapped = std::get_if<count_value_t>(&count_int_result); !count_wrapped) {
            return std::get<result_t>(count_int_result);
        } else {

            auto head = from + (count_wrapped->consumed - already_consumed);
            size_t left = std::distance(head, to);
            size_t count = count_wrapped->value;
            auto terminator_size = terminator.size;
            if (left < count + terminator_size) {
                return not_enough_data_t{};
            }
            auto tail = head + count;
            auto tail_end = tail + terminator_size;

            bool found_terminator = terminator.equal(tail, tail_end);
            if (!found_terminator) {
                return protocol_error_t{
                    Error::make_error_code(bredis_errors::bulk_terminator)};
            }
            size_t consumed = count_wrapped->consumed + count + terminator_size;

            return helper::markup_string(consumed, head, tail);
	}
    }
};

template <typename Iterator, typename Policy> struct array_parser_t {
    static auto apply(const Iterator &from, const Iterator &to,
                      size_t already_consumed)
        -> parse_result_t<Iterator, Policy> {

        using helper = markup_helper_t<Iterator, Policy>;
        using count_unwrapper_t = unwrap_count_t<Iterator, Policy>;
        using result_t = parse_result_t<Iterator, Policy>;
        using keep_policy = parsing_policy::keep_result;
        using count_parser_t = string_parser_t<Iterator, keep_policy>;
        using array_helper = array_helper_t<Iterator, Policy>;
        using element_t = parse_result_mapper_t<Iterator, Policy>;

        auto count_result = count_parser_t::apply(from, to, already_consumed);
        auto count_int_result =
            std::visit(count_unwrapper_t{}, count_result);
        if (auto *count_wrapped = std::get_if<count_value_t>(&count_int_result); !count_wrapped) {
            return std::get<result_t>(count_int_result);
        } else {
            auto count = count_wrapped->value;
            array_helper elements{count_wrapped->consumed, count};
            long marked_elements{0};
            Iterator element_from =
                from + (count_wrapped->consumed - already_consumed);
            while (marked_elements < count) {
                auto element_result = raw_parse<Iterator, Policy>(element_from, to);
                if (auto *element = std::get_if<element_t>(&element_result); !element) {
                    return element_result;
                } else {
                    element_from += element->consumed;
                    elements.push(*element);
                    ++marked_elements;
		}
	    }
            return elements.get();
	}
    }
};

template <typename Iterator, typename Policy>
using primary_parser_t = std::variant<
    not_enough_data_t, protocol_error_t, string_parser_t<Iterator, Policy>,
    int_parser_t<Iterator, Policy>, error_parser_t<Iterator, Policy>,
    bulk_string_parser_t<Iterator, Policy>, array_parser_t<Iterator, Policy>>;

template <typename Iterator, typename Policy>
struct unwrap_primary_parser_t {
    using wrapped_result_t = parse_result_t<Iterator, Policy>;

    const Iterator &from_;
    const Iterator &to_;

    unwrap_primary_parser_t(const Iterator &from, const Iterator &to)
        : from_{from}, to_{to} {}

    wrapped_result_t operator()(const not_enough_data_t &value) const {
        return value;
    }

    wrapped_result_t operator()(const protocol_error_t &value) const {
        return value;
    }

    template <typename Parser>
    wrapped_result_t operator()(const Parser &ignored) const {
        auto next_from = from_ + 1;
        return Parser::apply(next_from, to_, 1);
    }
};

template <typename Iterator, typename Policy>
struct construct_primary_parcer_t {
    using result_t = primary_parser_t<Iterator, Policy>;

    static auto apply(const Iterator &from, const Iterator &to) -> result_t {
        if (from == to) {
            return not_enough_data_t{};
        }

        switch (*from) {
        case '+': {
            return string_parser_t<Iterator, Policy>{};
        }
        case '-': {
            return error_parser_t<Iterator, Policy>{};
        }
        case ':': {
            return int_parser_t<Iterator, Policy>{};
        }
        case '$': {
            return bulk_string_parser_t<Iterator, Policy>{};
        }
        case '*': {
            return array_parser_t<Iterator, Policy>{};
        }
        }
        // wrong introduction;
        return protocol_error_t{
            Error::make_error_code(bredis_errors::wrong_intoduction)};
    }
};

template <typename Iterator, typename Policy>
parse_result_t<Iterator, Policy> raw_parse(const Iterator &from,
                                           const Iterator &to) {
    auto primary =
        construct_primary_parcer_t<Iterator, Policy>::apply(from, to);
    return std::visit(
        unwrap_primary_parser_t<Iterator, Policy>(from, to), primary);
}

} // namespace details

template <typename Iterator, typename Policy>
parse_result_t<Iterator, Policy> Protocol::parse(const Iterator &from,
                                                 const Iterator &to) {
    return details::raw_parse<Iterator, Policy>(from, to);
}

std::ostream &Protocol::serialize(std::ostream &buff,
                                  const single_command_t &cmd) {
    buff << '*' << (cmd.arguments.size()) << terminator;

    for (const auto &arg : cmd.arguments) {
        buff << '$' << arg.size() << terminator << arg << terminator;
    }
    return buff;
}

} // namespace bredis
