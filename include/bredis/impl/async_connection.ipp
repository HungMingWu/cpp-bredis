//
//
// Copyright (c) 2017 Ivan Baidakou (basiliscos) (the dot dmol at gmail dot com)
//
// Distributed under the MIT Software License
//
#pragma once

#include <algorithm>
#include <cassert>
#include <type_traits>

#include "common.ipp"

namespace bredis {

template <typename AsyncStream>
template <typename WriteCallback, typename... Args>
void AsyncConnection<AsyncStream>::async_write(const std::string &cmd,
                                               WriteCallback write_callback,
                                               Args &&... args) {
    namespace asio = boost::asio;
    namespace sys = boost::system;

    std::stringstream out;
    args_container_t command_args{args...};
    Protocol::serialize(out, cmd, command_args);

    auto str = std::make_shared<std::string>(std::move(out.str()));
    auto str_ptr = str.get();
    BREDIS_LOG_DEBUG("async_write >> " << str_ptr->c_str());
    asio::const_buffers_1 output_buf =
        asio::buffer(str_ptr->c_str(), str_ptr->size());

    asio::async_write(socket_, output_buf,
                      [str, write_callback](const sys::error_code &error_code,
                                            std::size_t bytes_transferred) {
                          write_callback(error_code);
                      });
}

template <typename AsyncStream>
template <typename ReadCallback, typename Buffer>
void AsyncConnection<AsyncStream>::async_read(Buffer &rx_buff,
                                              ReadCallback read_callback) {

    namespace asio = boost::asio;
    namespace sys = boost::system;

    BREDIS_LOG_DEBUG("async_read");

    asio::async_read_until(
        socket_, rx_buff, match_result,
        [read_callback, &rx_buff](const sys::error_code &error_code,
                                  std::size_t bytes_transferred) {
            if (error_code) {
                read_callback(error_code, {}, 0);
                return;
            }

            auto const_buff = rx_buff.data();
            const char *char_ptr =
                boost::asio::buffer_cast<const char *>(const_buff);
            auto size = rx_buff.size();
            string_t data(char_ptr, size);
            BREDIS_LOG_DEBUG("incoming data(" << size << ") : " << char_ptr << ", tx bytes: " << bytes_transferred);

            auto parse_result = Protocol::parse(data);
            boost::system::error_code ec;
            if (parse_result.consumed == 0) {
                /* might happen only in case of protocol error */
                protocol_error_t protocol_error =
                    boost::get<protocol_error_t>(parse_result.result);
                BREDIS_LOG_DEBUG("protocol error: " << protocol_error.what);
                auto parse_error_code =
                    Error::make_error_code(bredis_errors::protocol_error);
                read_callback(parse_error_code, {}, 0);
                return;
            }

            redis_result_t redis_result = boost::apply_visitor(
                some_result_visitor(), parse_result.result);
            read_callback(ec, std::move(redis_result), parse_result.consumed);
        });
}

} // namespace bredis