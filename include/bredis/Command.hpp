//
//
// Copyright (c) 2017 Ivan Baidakou (basiliscos) (the dot dmol at gmail dot com)
//
// Distributed under the MIT Software License
//
#pragma once

#include <string_view>
#include <type_traits>
#include <vector>
#include <variant>

#include "Result.hpp"

namespace bredis {

namespace detail {

template <bool...> struct bool_pack;
template <bool... bs>
using all_true = std::is_same<bool_pack<bs..., true>, bool_pack<true, bs...>>;

template <class R, class... Ts>
using are_all_constructible = all_true<std::is_constructible<R, Ts>::value...>;

} // namespace detail

using args_container_t = std::vector<std::string_view>;
struct single_command_t {
    args_container_t arguments;

    template <typename... Args,
              typename = std::enable_if_t<detail::are_all_constructible<
                  std::string_view, Args...>::value>>
    single_command_t(Args &&... args) : arguments{std::forward<Args>(args)...} {
        static_assert(sizeof...(Args) >= 1, "Empty command is not allowed");
    }

    template <typename InputIterator,
              typename = std::enable_if_t<std::is_constructible<
                  std::string_view, typename std::iterator_traits<
                                         InputIterator>::value_type>::value>>
    single_command_t(InputIterator first, InputIterator last)
        : arguments(first, last) {}
};

using command_container_t = std::vector<single_command_t>;

using command_wrapper_t = std::variant<single_command_t, command_container_t>;

} // namespace bredis
