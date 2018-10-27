//
//
// Copyright (c) 2017 Ivan Baidakou (basiliscos) (the dot dmol at gmail dot com)
//
// Distributed under the MIT Software License
//
#pragma once

#include <algorithm>
#include <boost/convert.hpp>
#include <boost/convert/lexical_cast.hpp>
#include <cctype>
#include <iterator>
#include <string>

#include "Command.hpp"
#include "Markers.hpp"

namespace bredis {

namespace marker_helpers {

struct stringizer {

    std::string operator()(const markers::string_t &value) const {
        std::string r(value);
        return "[str] " + r;
    }

    std::string operator()(const markers::error_t &value) const {
        std::string r(value);;
        return "[err] " + r;
    }

    std::string operator()(const markers::int_t &value) const {
        std::string r(value);
        return "[int] " + r;
    }

    std::string operator()(const markers::nil_t &value) const {
        return "[nil] ";
    }

    std::string
    operator()(const markers::array_holder_t &value) const {
        std::string r = "[array] {";
        for (const auto &v : value.elements) {
            r += std::visit(*this, v) + ", ";
        }
        r += "}";
        return r;
    }
};

class equality {
    using StringIterator = std::string::const_iterator;

  private:
    std::string copy_;
    StringIterator begin_;
    StringIterator end_;

  public:
    equality(std::string str)
        : copy_(str), begin_(std::begin(copy_)), end_(std::end(copy_)) {}

    template <typename T> bool operator()(const T &value) const {
        return false;
    }

    bool operator()(const markers::string_t &value) const {
        auto helper = stringizer();
        auto str = helper(value);
        return std::equal(begin_, end_, value.begin(), value.end());
    }

    bool operator()(const markers::int_t &value) const {
        return std::equal(begin_, end_, value.begin(), value.end());
    }

    bool operator()(const markers::error_t &value) const {
        return std::equal(begin_, end_, value.begin(), value.end());
    }

    bool operator()(const markers::nil_t &value) const {
        return std::equal(begin_, end_, value.begin(), value.end());
    }
};

// Auxillary class, that scans redis parse results for the matching
// of the strings provided in constructor.
//
// We need to check subscription confirmation from redis, as it comes
// from redis in a form
// [[string] "subscribe", [string] channel_name, [int] subscribes_count]
// we check only first two fields (by string equality) and ignore the
// last (as we usually do not care)
//

class check_subscription {

  private:
    bredis::single_command_t cmd_;

  public:
    template <typename Command>
    check_subscription(Command &&cmd) : cmd_{std::forward<Command>(cmd)} {}

    template <typename T> bool operator()(const T &value) const {
        return false;
    }

    bool
    operator()(const bredis::markers::array_holder_t &value) const {
        if ((value.elements.size() == 3) && (cmd_.arguments.size() >= 2)) {
            // check case-insentensive 1st argument, which chan be subscribe or
            // psubscribe
            const auto *cmd = std::get_if<bredis::markers::string_t>(
                &value.elements[0]);
            if (!cmd) {
                return false;
            }
            bool eq_cmd = std::equal(
                cmd->begin(), cmd->end(), cmd_.arguments[0].cbegin(),
                [](const char a, const char b) {
                    return std::toupper(static_cast<unsigned char>(a)) ==
                           std::toupper(static_cast<unsigned char>(b));
                });
            if (!eq_cmd) {
                return false;
            }

            // get the index, 3rd field as string
            const auto *idx_ref = std::get_if<bredis::markers::int_t>(
                &value.elements[2]);
            if (!idx_ref) {
                return false;
            }

            std::string idx_str{*idx_ref};
            boost::cnv::lexical_cast cnv;
            auto idx_option = boost::convert<int>(idx_str, cnv);
            if (!idx_option) {
                return false;
            }
            int idx = idx_option.value();
            int size = static_cast<int>(cmd_.arguments.size());
            // out of scope
            if (idx < 1 || idx >= size) {
                return false;
            }

            // case-sentensive channel name comparison
            const auto *channel =
                std::get_if<bredis::markers::string_t>(
                    &value.elements[1]);
            if (!channel) {
                return false;
            }

            const auto &channel_ = cmd_.arguments[idx];
            return std::equal(channel_.cbegin(), channel_.cend(), channel->begin(),
                              channel->end());
        }
        return false;
    }
};

} // marker_helpers

} // bredis
