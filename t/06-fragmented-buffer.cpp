#include <boost/asio/buffer.hpp>
#include <boost/asio/streambuf.hpp>
#include <sstream>
#include <vector>

#include "bredis/MarkerHelpers.hpp"
#include "bredis/Protocol.hpp"

#include "catch.hpp"

namespace r = bredis;
namespace asio = boost::asio;

TEST_CASE("right consumption", "[protocol]") {
    using Buffer = std::vector<asio::const_buffers_1>;
    using Iterator = boost::asio::buffers_iterator<Buffer, char>;
    using Policy = r::parsing_policy::keep_result;
    using positive_result_t = r::parse_result_mapper_t<Policy>;

    std::string full_message =
        "*3\r\n$7\r\nmessage\r\n$13\r\nsome-channel1\r\n$10\r\nmessage-a1\r\n";

    Buffer buff;
    for (auto i = 0; i < full_message.size(); i++) {
        asio::const_buffers_1 v(full_message.c_str() + i, 1);
        buff.push_back(v);
    }
    auto b_from = Iterator::begin(buff), b_to = Iterator::end(buff);
	std::string_view view;
    auto parsed_result = r::Protocol::parse(view);
    auto positive_parse_result = std::get<positive_result_t>(parsed_result);

    REQUIRE(positive_parse_result.consumed);
    REQUIRE(positive_parse_result.consumed == full_message.size());

    auto *array = std::get_if<r::markers::array_holder_t>(
        &positive_parse_result.result);
    REQUIRE(array != nullptr);
    REQUIRE(array->elements.size() == 3);

    REQUIRE(std::visit(
        r::marker_helpers::equality("message"), array->elements[0]));
    REQUIRE(std::visit(
        r::marker_helpers::equality("some-channel1"),
        array->elements[1]));
    REQUIRE(std::visit(
        r::marker_helpers::equality("message-a1"),
        array->elements[2]));
    REQUIRE(std::get_if<r::markers::string_t>(&array->elements[0]) !=
            nullptr);
    REQUIRE(std::get_if<r::markers::string_t>(&array->elements[1]) !=
            nullptr);
    REQUIRE(std::get_if<r::markers::string_t>(&array->elements[2]) !=
            nullptr);
}

TEST_CASE("using strembuff", "[protocol]") {
    using Buffer = boost::asio::streambuf;
    using Iterator = typename r::to_iterator<Buffer>::iterator_t;
    using Policy = r::parsing_policy::keep_result;
    using positive_result_t = r::parse_result_mapper_t<Policy>;

    std::string ok = "+OK\r\n";
    Buffer buff;
    std::ostream os(&buff);
    os.write(ok.c_str(), ok.size());

	std::string_view view(boost::asio::buffer_cast<const char*>(buff.data()), buff.size());
    auto parsed_result = r::Protocol::parse(view);

    auto positive_parse_result = std::get<positive_result_t>(parsed_result);
    REQUIRE(positive_parse_result.consumed == ok.size());
    REQUIRE(std::visit(r::marker_helpers::equality("OK"),
                                 positive_parse_result.result));
}
