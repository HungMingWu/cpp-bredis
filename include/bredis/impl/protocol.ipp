//
//
// Copyright (c) 2017 Ivan Baidakou (basiliscos) (the dot dmol at gmail dot com)
//
// Distributed under the MIT Software License
//
#pragma once

#include <algorithm>
#include <boost/lexical_cast.hpp>
#include <variant>
#include <errno.h>
#include <stdlib.h>
#include <string>

namespace bredis {

namespace {
constexpr std::string_view terminator{"\r\n"};
}

namespace details {

// forward declaration
template <typename Policy>
parse_result_t<Policy> raw_parse(std::string_view view);

struct count_value_t {
    size_t value;
    size_t consumed;
};

template <typename Policy>
using count_variant_t =
    std::variant<count_value_t, parse_result_t<Policy>>;

template <typename Policy> struct markup_helper_t {
    using result_wrapper_t = parse_result_t<Policy>;
    using positive_wrapper_t = parse_result_mapper_t<Policy>;
    using result_t = markers::redis_result_t;
    using string_t = markers::string_t;

    static auto markup_string(size_t consumed, std::string_view view) -> result_wrapper_t {
        return positive_wrapper_t{
            result_t{markers::string_t{view}}, consumed};
    }

    static auto markup_nil(size_t consumed, const string_t &str)
        -> result_wrapper_t {
        return positive_wrapper_t{
            result_t{markers::nil_t{str}}, consumed};
    }

    static auto markup_error(positive_wrapper_t &wrapped_string)
        -> result_wrapper_t {
        auto &str = std::get<string_t>(wrapped_string.result);
        return positive_wrapper_t{result_t{markers::error_t{str}},
                               wrapped_string.consumed};
    }

    static auto markup_int(positive_wrapper_t &wrapped_string)
        -> result_wrapper_t {
        auto &str = std::get<string_t>(wrapped_string.result);
        return positive_wrapper_t{
            result_t{markers::int_t{str}}, wrapped_string.consumed};
    }
};

template <>
struct markup_helper_t<parsing_policy::drop_result> {
    using policy_t = parsing_policy::drop_result;
    using result_wrapper_t = parse_result_t<policy_t>;
    using positive_wrapper_t = parse_result_mapper_t<policy_t>;
    using string_t = markers::string_t;

    static auto markup_string(size_t consumed, std::string_view view) -> result_wrapper_t {
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

template <typename Policy> struct array_helper_t {
    using policy_t = Policy;
    using array_t = markers::array_holder_t;
    using item_t = parse_result_mapper_t<policy_t>;
    using result_t = parse_result_t<policy_t>;

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

template <>
struct array_helper_t<parsing_policy::drop_result> {
    using policy_t = parsing_policy::drop_result;
    using array_t = markers::array_holder_t;
    using item_t = parse_result_mapper_t<policy_t>;
    using result_t = parse_result_t<policy_t>;

    size_t consumed_;

    array_helper_t(size_t consumed, size_t count) : consumed_{consumed} {}

    void push(const item_t &item) { consumed_ += item.consumed; }

    result_t get() { return result_t{item_t{consumed_}}; }
};

template <typename Policy>
struct unwrap_count_t {
    using wrapped_result_t = count_variant_t<Policy>;
    using negative_result_t = parse_result_t<Policy>;
    using positive_input_t =
        parse_result_mapper_t<parsing_policy::keep_result>;

    template <typename T> wrapped_result_t operator()(const T &value) const {
        return negative_result_t{value};
    }

    wrapped_result_t operator()(const positive_input_t &value) const {
        using string_t = markers::string_t;
        using helper = markup_helper_t<Policy>;

        auto &count_string_ref = std::get<string_t>(value.result);
        std::string count_string{count_string_ref};
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

template <typename Policy> struct string_parser_t {

    static auto apply(std::string_view view, size_t already_consumed)
        -> parse_result_t<Policy> {
        using helper = markup_helper_t<Policy>;

		auto found_terminator = view.find(terminator);

        if (found_terminator == std::string_view::npos) {
            return not_enough_data_t{};
        }

        size_t consumed = already_consumed + terminator.size() + found_terminator;
        return helper::markup_string(consumed, view.substr(0, found_terminator));
    }
};

template <typename Policy> struct error_parser_t {
    static auto apply(std::string_view view, size_t already_consumed)
        -> parse_result_t<Policy> {
        using helper = markup_helper_t<Policy>;
        using parser_t = string_parser_t<Policy>;
        using wrapped_result_t = parse_result_mapper_t<Policy>;

        auto result = parser_t::apply(view, already_consumed);
        if (auto *wrapped_string = std::get_if<wrapped_result_t>(&result); !wrapped_string) {
            return result;
        } else {
	        return helper::markup_error(*wrapped_string);
	    }
    }
};

template <typename Policy> struct int_parser_t {
    static auto apply(std::string_view view, size_t already_consumed)
        -> parse_result_t<Policy> {
        using helper = markup_helper_t<Policy>;
        using parser_t = string_parser_t<Policy>;
        using wrapped_result_t = parse_result_mapper_t<Policy>;

        auto result = parser_t::apply(view, already_consumed);
        if (auto *wrapped_string = std::get_if<wrapped_result_t>(&result); !wrapped_string) {
            return result;
        } else {
            return helper::markup_int(*wrapped_string);
	    }
    }
};

template <typename Policy> struct bulk_string_parser_t {

    static auto apply(std::string_view view, size_t already_consumed)
        -> parse_result_t<Policy> {

        using helper = markup_helper_t<Policy>;
        using count_unwrapper_t = unwrap_count_t<Policy>;
        using keep_policy = parsing_policy::keep_result;
        using count_parser_t = string_parser_t<keep_policy>;
        using result_t = parse_result_t<Policy>;

        auto count_result = count_parser_t::apply(view, already_consumed);
        auto count_int_result =
            std::visit(count_unwrapper_t{}, count_result);
        if (auto *count_wrapped = std::get_if<count_value_t>(&count_int_result); !count_wrapped) {
            return std::get<result_t>(count_int_result);
        } else {
            auto head = count_wrapped->consumed - already_consumed;
            size_t left = view.size() - head;
            size_t count = count_wrapped->value;
            auto terminator_size = terminator.size();
            if (left < count + terminator_size) {
                return not_enough_data_t{};
            }
            auto tail = view.substr(head + count, terminator.size());

			bool found_terminator = terminator == tail;
            if (!found_terminator) {
                return protocol_error_t{
                    Error::make_error_code(bredis_errors::bulk_terminator)};
            }
            size_t consumed = count_wrapped->consumed + count + terminator_size;

			std::string_view view;// { head, count };
            return helper::markup_string(consumed, view);
		}
    }
};

template <typename Policy> struct array_parser_t {
    static auto apply(std::string_view view, size_t already_consumed)
        -> parse_result_t<Policy> {

        using helper = markup_helper_t<Policy>;
        using count_unwrapper_t = unwrap_count_t<Policy>;
        using result_t = parse_result_t<Policy>;
        using keep_policy = parsing_policy::keep_result;
        using count_parser_t = string_parser_t<keep_policy>;
        using array_helper = array_helper_t<Policy>;
        using element_t = parse_result_mapper_t<Policy>;

        auto count_result = count_parser_t::apply(view, already_consumed);
        auto count_int_result =
            std::visit(count_unwrapper_t{}, count_result);
        if (auto *count_wrapped = std::get_if<count_value_t>(&count_int_result); !count_wrapped) {
            return std::get<result_t>(count_int_result);
		}
		else {
			auto count = count_wrapped->value;
			array_helper elements{ count_wrapped->consumed, count };
			long marked_elements{ 0 };
			auto element_from = count_wrapped->consumed - already_consumed;
			while (marked_elements < count) {
				auto element_result = raw_parse<Policy>(view.substr(element_from));
				if (auto *element = std::get_if<element_t>(&element_result); !element) {
					return element_result;
				}
				else {
					element_from += element->consumed;
					elements.push(*element);
					++marked_elements;
				}
			}
			return elements.get();
		}
    }
};

template <typename Policy>
using primary_parser_t = std::variant<
    not_enough_data_t, protocol_error_t, string_parser_t<Policy>,
    int_parser_t<Policy>, error_parser_t<Policy>,
    bulk_string_parser_t<Policy>, array_parser_t<Policy>>;

template <typename Policy>
struct unwrap_primary_parser_t {
    using wrapped_result_t = parse_result_t<Policy>;

	std::string_view from_;
    unwrap_primary_parser_t(std::string_view from)
        : from_{from} {}

    wrapped_result_t operator()(const not_enough_data_t &value) const {
        return value;
    }

    wrapped_result_t operator()(const protocol_error_t &value) const {
        return value;
    }

    template <typename Parser>
    wrapped_result_t operator()(const Parser &ignored) const {
        return Parser::apply(from_.substr(1), 1);
    }
};

template <typename Policy>
struct construct_primary_parcer_t {
    using result_t = primary_parser_t<Policy>;

    static auto apply(std::string_view view) -> result_t {
        if (view.empty()) {
            return not_enough_data_t{};
        }

        switch (view[0]) {
        case '+': {
            return string_parser_t<Policy>{};
        }
        case '-': {
            return error_parser_t<Policy>{};
        }
        case ':': {
            return int_parser_t<Policy>{};
        }
        case '$': {
            return bulk_string_parser_t<Policy>{};
        }
        case '*': {
            return array_parser_t<Policy>{};
        }
        }
        // wrong introduction;
        return protocol_error_t{
            Error::make_error_code(bredis_errors::wrong_intoduction)};
    }
};

template <typename Policy>
parse_result_t<Policy> raw_parse(std::string_view view) {
    auto primary =
        construct_primary_parcer_t<Policy>::apply(view);
    return std::visit(
        unwrap_primary_parser_t<Policy>(view), primary);
}

} // namespace details

template <typename Policy>
parse_result_t<Policy> Protocol::parse(std::string_view view) {
    return details::raw_parse<Policy>(view);
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
