//
//
// Copyright (c) 2017 Ivan Baidakou (basiliscos) (the dot dmol at gmail dot com)
//
// Distributed under the MIT Software License
//
#pragma once

#include <iostream>
#include <locale>
#include <sstream>
#include <string>

#ifdef BREDIS_DEBUG
#define BREDIS_LOG_DEBUG(msg)                                                  \
    { std::cout << __FILE__ << ":" << __LINE__ << " :: " << msg << std::endl; };
#else
#define BREDIS_LOG_DEBUG(msg) ;
#endif

namespace bredis {

struct consumed_parse {
	template <typename Policy>
    int operator()(const positive_parse_result_t<Policy> &value) const {
        return static_cast<int>(value.consumed);
    }

    int operator()(const not_enough_data_t &value) const { return 0; }

    int operator()(const protocol_error_t &value) const { return -1; }
};

// todo:  change to std::basic_string_view<charT> in C++17
template <typename charT> using basic_string_view_type = std::basic_string_view<charT>;

// Creates a string view from a pair of iterators
//  http://stackoverflow.com/q/33750600/882436
template <typename _It>
inline constexpr auto make_string_view( _It begin, _It end )
{
    using result_type = basic_string_view_type<typename std::iterator_traits<_It>::value_type>;

    return result_type{
        ( begin != end ) ? &*begin : nullptr
        ,  (typename result_type::size_type)
        std::max(
            std::distance(begin, end)
            , (typename result_type::difference_type)0
        )
     };
}   // make_string_view

template <typename Iterator> class MatchResult {
  private:
    std::size_t matched_results_;
    std::size_t expected_count_;

  public:
    MatchResult(std::size_t expected_count)
        : expected_count_(expected_count), matched_results_(0) {}

    std::pair<Iterator, bool> operator()(Iterator begin, Iterator end) {
        using Policy = bredis::parsing_policy::drop_result;

        auto parsing_complete = false;
        size_t consumed = 0;
        auto parse_from = begin;
        do {
            auto from = parse_from;
            auto view = make_string_view(begin, end);
            auto parse_result = Protocol::parse<Policy>(view);
            auto consumable = std::visit(
                consumed_parse(), parse_result);
            if (consumable == -1) {
                // parse error
                consumed = 0;
                parsing_complete = true;
                break;
            }
            if (consumable == 0) {
                // no enough data
                break;
            }
            ++matched_results_;
            auto iteration_consumed = static_cast<std::size_t>(consumable);
            consumed += iteration_consumed;
            parse_from += iteration_consumed;
            parsing_complete = (matched_results_ == expected_count_);
        } while (!parsing_complete);
        return std::make_pair(begin + consumed, parsing_complete);
    }
};

class command_serializer_visitor {
  public:
    std::string operator()(const single_command_t &value) const {
        std::stringstream out;
        out.imbue(std::locale::classic());
        Protocol::serialize(out, value);
        return out.str();
    }

    std::string operator()(const command_container_t &value) const {
        std::stringstream out;
        out.imbue(std::locale::classic());
        for (const auto &cmd : value) {
            Protocol::serialize(out, cmd);
        }
        return out.str();
    }
};

} // namespace bredis

namespace boost {
namespace asio {

template <typename Iterator>
struct is_match_condition<bredis::MatchResult<Iterator>>
    : public std::true_type {};
}
}
