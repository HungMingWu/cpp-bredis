#include <vector>

#include "bredis/MarkerHelpers.hpp"
#include "bredis/Protocol.hpp"
#include "catch.hpp"

namespace r = bredis;
namespace asio = boost::asio;

using Policy = r::parsing_policy::keep_result;
using positive_result_t = r::parse_result_mapper_t<Policy>;

TEST_CASE("simple string", "[protocol]") {
    std::string ok = "+OK\r\n";
    auto parsed_result = r::Protocol::parse(ok);
    auto positive_parse_result = std::get<positive_result_t>(parsed_result);
    REQUIRE(positive_parse_result.consumed == ok.size());
    REQUIRE(std::visit(r::marker_helpers::equality("OK"),
                                 positive_parse_result.result));
};

TEST_CASE("empty string", "[protocol]") {
    std::string ok = "";
    auto parsed_result = r::Protocol::parse(ok);
    r::not_enough_data_t *r = std::get_if<r::not_enough_data_t>(&parsed_result);
    REQUIRE(r != nullptr);
};

TEST_CASE("non-finished ", "[protocol]") {
    std::string ok = "+OK";
    auto parsed_result = r::Protocol::parse(ok);
    r::not_enough_data_t *r = std::get_if<r::not_enough_data_t>(&parsed_result);
    REQUIRE(r != nullptr);
};

TEST_CASE("wrong start marker", "[protocol]") {
    std::string ok = "!OK";
    auto parsed_result = r::Protocol::parse(ok);
    r::protocol_error_t *r = std::get_if<r::protocol_error_t>(&parsed_result);
    REQUIRE(r->message() == "Wrong introduction");
};

TEST_CASE("number-like", "[protocol]") {
    std::string ok = ":-55abc\r\n";
    auto parsed_result = r::Protocol::parse(ok);
    auto positive_parse_result = std::get<positive_result_t>(parsed_result);
    REQUIRE(positive_parse_result.consumed);
    REQUIRE(positive_parse_result.consumed == ok.size());
    REQUIRE(std::get_if<r::markers::int_t>(
                &positive_parse_result.result) != nullptr);
    REQUIRE(
        std::visit(r::marker_helpers::equality("-55abc"),
                             positive_parse_result.result));
};

TEST_CASE("no enough data for number", "[protocol]") {
    std::string ok = ":55\r";
    auto parsed_result = r::Protocol::parse(ok);
    r::not_enough_data_t *r = std::get_if<r::not_enough_data_t>(&parsed_result);
    REQUIRE(r != nullptr);
};

TEST_CASE("simple error", "[protocol]") {
    std::string ok = "-Ooops\r\n";
    auto parsed_result = r::Protocol::parse(ok);
    auto positive_parse_result = std::get<positive_result_t>(parsed_result);
    REQUIRE(positive_parse_result.consumed);
    REQUIRE(positive_parse_result.consumed == ok.size());
    REQUIRE(std::get_if<r::markers::error_t>(
                &positive_parse_result.result) != nullptr);
    REQUIRE(std::visit(r::marker_helpers::equality("Ooops"),
                                 positive_parse_result.result));
};

TEST_CASE("no enoght data for error", "[protocol]") {
    std::string ok = "-Ooops";
    auto parsed_result = r::Protocol::parse(ok);
    r::not_enough_data_t *r = std::get_if<r::not_enough_data_t>(&parsed_result);
    REQUIRE(r != nullptr);
};

TEST_CASE("nil", "[protocol]") {
    std::string ok = "$-1\r\n";
    auto parsed_result = r::Protocol::parse(ok);
    auto positive_parse_result = std::get<positive_result_t>(parsed_result);
    REQUIRE(positive_parse_result.consumed);
    REQUIRE(positive_parse_result.consumed == ok.size());

    auto *nil =
        std::get_if<r::markers::nil_t>(&positive_parse_result.result);
    REQUIRE(nil != nullptr);
    REQUIRE(std::visit(r::marker_helpers::equality("-1"),
                                 positive_parse_result.result));
};

TEST_CASE("malformed bulk string", "[protocol]") {
    std::string ok = "$-5\r\nsome\r\n";
    auto parsed_result = r::Protocol::parse(ok);
    r::protocol_error_t *r = std::get_if<r::protocol_error_t>(&parsed_result);
    REQUIRE(r->message() == "Unacceptable count value");
};

TEST_CASE("some bulk string", "[protocol]") {
    std::string ok = "$4\r\nsome\r\n";
    auto parsed_result = r::Protocol::parse(ok);
    auto positive_parse_result = std::get<positive_result_t>(parsed_result);
    REQUIRE(positive_parse_result.consumed);
    REQUIRE(positive_parse_result.consumed == ok.size());
    REQUIRE(std::get_if<r::markers::string_t>(
                &positive_parse_result.result) != nullptr);
    REQUIRE(std::visit(r::marker_helpers::equality("some"),
                                 positive_parse_result.result));
};

TEST_CASE("empty bulk string", "[protocol]") {
    std::string ok = "$0\r\n\r\n";
    auto parsed_result = r::Protocol::parse(ok);
    auto positive_parse_result = std::get<positive_result_t>(parsed_result);
    REQUIRE(positive_parse_result.consumed);
    REQUIRE(positive_parse_result.consumed == ok.size());
    REQUIRE(std::get_if<r::markers::string_t>(
                &positive_parse_result.result) != nullptr);
    REQUIRE(std::visit(r::marker_helpers::equality(""),
                                 positive_parse_result.result));
};

TEST_CASE("patrial bulk string(1)", "[protocol]") {
    std::string ok = "$10\r\nsome\r\n";
    auto parsed_result = r::Protocol::parse(ok);
    REQUIRE(std::get_if<r::not_enough_data_t>(&parsed_result) != nullptr);
};

TEST_CASE("patrial bulk string(2)", "[protocol]") {
    std::string ok = "$4\r\nsome\r";
    auto parsed_result = r::Protocol::parse(ok);
    REQUIRE(std::get_if<r::not_enough_data_t>(&parsed_result) != nullptr);
};

TEST_CASE("patrial bulk string(3)", "[protocol]") {
    std::string ok = "$4\r";
    auto parsed_result = r::Protocol::parse(ok);
    REQUIRE(std::get_if<r::not_enough_data_t>(&parsed_result) != nullptr);
};

TEST_CASE("patrial bulk string(4)", "[protocol]") {
    using Policy = r::parsing_policy::drop_result;
    std::string ok = "$4\r\nsome\r";
    auto parsed_result = r::Protocol::parse<Policy>(ok);
    REQUIRE(std::get_if<r::not_enough_data_t>(&parsed_result) != nullptr);
};

TEST_CASE("malformed bulk string(2)", "[protocol]") {
    std::string ok = "$1\r\nsome\r\n";
    auto parsed_result = r::Protocol::parse(ok);
    r::protocol_error_t *r = std::get_if<r::protocol_error_t>(&parsed_result);
    REQUIRE(r->message() == "Terminator for bulk string not found");
};

TEST_CASE("malformed bulk string(3)", "[protocol]") {
    using Policy = r::parsing_policy::drop_result;
    std::string ok = "$4\r\nsomemm";
    auto parsed_result = r::Protocol::parse<Policy>(ok);
    r::protocol_error_t *r = std::get_if<r::protocol_error_t>(&parsed_result);
    REQUIRE(r->message() == "Terminator for bulk string not found");
};

TEST_CASE("malformed bulk string(4)", "[protocol]") {
    using Policy = r::parsing_policy::drop_result;
    std::string ok = "$36893488147419103232\r\nsomemm";
    auto parsed_result = r::Protocol::parse<Policy>(ok);
    r::protocol_error_t *r = std::get_if<r::protocol_error_t>(&parsed_result);
    REQUIRE(r->message() == "Cannot convert count to number");
};

TEST_CASE("empty array", "[protocol]") {
    std::string ok = "*0\r\n";
    auto parsed_result = r::Protocol::parse(ok);
    auto positive_parse_result = std::get<positive_result_t>(parsed_result);
    REQUIRE(positive_parse_result.consumed);
    REQUIRE(positive_parse_result.consumed == ok.size());
    auto *array_holder = std::get_if<r::markers::array_holder_t>(
        &positive_parse_result.result);
    REQUIRE(array_holder != nullptr);
    REQUIRE(array_holder->elements.size() == 0);
};

TEST_CASE("null array", "[protocol]") {
    std::string ok = "*-1\r\n";
    auto parsed_result = r::Protocol::parse(ok);
    auto positive_parse_result = std::get<positive_result_t>(parsed_result);
    REQUIRE(positive_parse_result.consumed);
    REQUIRE(positive_parse_result.consumed == ok.size());
    auto *nil =
        std::get_if<r::markers::nil_t>(&positive_parse_result.result);
    REQUIRE(nil != nullptr);
    REQUIRE(std::visit(r::marker_helpers::equality("-1"),
                                 positive_parse_result.result));
};

TEST_CASE("malformed array", "[protocol]") {
    std::string ok = "*-4\r\nsome\r\n";
    auto parsed_result = r::Protocol::parse(ok);
    r::protocol_error_t *r = std::get_if<r::protocol_error_t>(&parsed_result);
    REQUIRE(r->message() == "Unacceptable count value");
};

TEST_CASE("malformed array (2)", "[protocol]") {
    std::string ok = "*36893488147419103232\r\nsome\r\n";
    auto parsed_result = r::Protocol::parse(ok);
    r::protocol_error_t *r = std::get_if<r::protocol_error_t>(&parsed_result);
    REQUIRE(r->message() == "Cannot convert count to number");
};

TEST_CASE("patrial array(1)", "[protocol]") {
    std::string ok = "*1\r\n";
    auto parsed_result = r::Protocol::parse(ok);
    REQUIRE(std::get_if<r::not_enough_data_t>(&parsed_result) != nullptr);
};

TEST_CASE("patrial array(2)", "[protocol]") {
    std::string ok = "*1";
    auto parsed_result = r::Protocol::parse(ok);
    REQUIRE(std::get_if<r::not_enough_data_t>(&parsed_result) != nullptr);
};

TEST_CASE("patrial array(3)", "[protocol]") {
    using Policy = r::parsing_policy::drop_result;
    std::string ok = "*1\r\n";
    auto parsed_result = r::Protocol::parse<Policy>(ok);
    REQUIRE(std::get_if<r::not_enough_data_t>(&parsed_result) != nullptr);
};

TEST_CASE("array: string, int, nil", "[protocol]") {
    std::string ok = "*3\r\n$4\r\nsome\r\n:5\r\n$-1\r\n";
    auto parsed_result = r::Protocol::parse(ok);
    auto positive_parse_result = std::get<positive_result_t>(parsed_result);
    REQUIRE(positive_parse_result.consumed);
    REQUIRE(positive_parse_result.consumed == ok.size());

    auto *array = std::get_if<r::markers::array_holder_t>(
        &positive_parse_result.result);
    REQUIRE(array != nullptr);
    REQUIRE(array->elements.size() == 3);

    REQUIRE(std::visit(r::marker_helpers::equality("some"),
                                 array->elements[0]));
    REQUIRE(std::visit(r::marker_helpers::equality("5"),
                                 array->elements[1]));
    REQUIRE(std::get_if<r::markers::int_t>(&array->elements[1]) !=
            nullptr);
    REQUIRE(std::get_if<r::markers::nil_t>(&array->elements[2]) !=
            nullptr);
};

TEST_CASE("array of arrays: [int, int, int,], [str,err] ", "[protocol]") {
    std::string ok = "*2\r\n*3\r\n:1\r\n:2\r\n:3\r\n*2\r\n+Foo\r\n-Bar\r\n";
    auto parsed_result = r::Protocol::parse(ok);
    auto positive_parse_result = std::get<positive_result_t>(parsed_result);
    REQUIRE(positive_parse_result.consumed);
    REQUIRE(positive_parse_result.consumed == ok.size());

    auto *array = std::get_if<r::markers::array_holder_t>(
        &positive_parse_result.result);
    REQUIRE(array != nullptr);
    REQUIRE(array->elements.size() == 2);

    auto *a1 =
        std::get_if<r::markers::array_holder_t>(&array->elements[0]);
    REQUIRE(a1->elements.size() == 3);
    REQUIRE(std::visit(r::marker_helpers::equality("1"),
                                 a1->elements[0]));
    REQUIRE(std::visit(r::marker_helpers::equality("2"),
                                 a1->elements[1]));
    REQUIRE(std::visit(r::marker_helpers::equality("3"),
                                 a1->elements[2]));
    REQUIRE(std::get_if<r::markers::int_t>(&a1->elements[0]) !=
            nullptr);
    REQUIRE(std::get_if<r::markers::int_t>(&a1->elements[1]) !=
            nullptr);
    REQUIRE(std::get_if<r::markers::int_t>(&a1->elements[2]) !=
            nullptr);

    auto *a2 =
        std::get_if<r::markers::array_holder_t>(&array->elements[1]);
    REQUIRE(a2->elements.size() == 2);
    REQUIRE(std::visit(r::marker_helpers::equality("Foo"),
                                 a2->elements[0]));
    REQUIRE(std::visit(r::marker_helpers::equality("Bar"),
                                 a2->elements[1]));
    REQUIRE(std::get_if<r::markers::string_t>(&a2->elements[0]) !=
            nullptr);
    REQUIRE(std::get_if<r::markers::error_t>(&a2->elements[1]) !=
            nullptr);
};

TEST_CASE("right consumption", "[protocol]") {
    std::string ok =
        "*3\r\n$7\r\nmessage\r\n$13\r\nsome-channel1\r\n$10\r\nmessage-a1\r\n";
    ok = ok + ok;
    auto parsed_result = r::Protocol::parse(ok);
    auto positive_parse_result = std::get<positive_result_t>(parsed_result);
    REQUIRE(positive_parse_result.consumed);
    REQUIRE(positive_parse_result.consumed == ok.size() / 2);
}

TEST_CASE("overfilled buffer", "[protocol]") {
    std::string ok = "*3\r\n$7\r\nmessage\r\n$13\r\nsome-channel1\r\n$"
                     "10\r\nmessage-a1\r\n*3\r\n$7\r\nmessage\r\n$13\r\nsome-"
                     "channel1\r\n$10\r\nmessage-a2\r\n*3\r\n$7\r\nmessage\r\n$"
                     "13\r\nsome-channel2\r\n$4\r\nlast\r\n";
    auto parsed_result = r::Protocol::parse(ok);
    auto positive_parse_result = std::get<positive_result_t>(parsed_result);
    REQUIRE(positive_parse_result.consumed);
    REQUIRE(positive_parse_result.consumed == 54);

    auto &a1 = std::get<r::markers::array_holder_t>(
        positive_parse_result.result);
    REQUIRE(a1.elements.size() == 3);
    REQUIRE(std::visit(
        r::marker_helpers::equality("message"), a1.elements[0]));
    REQUIRE(std::visit(
        r::marker_helpers::equality("some-channel1"),
        a1.elements[1]));
    REQUIRE(std::visit(
        r::marker_helpers::equality("message-a1"), a1.elements[2]));
    REQUIRE(std::get_if<r::markers::string_t>(&a1.elements[0]) !=
            nullptr);
    REQUIRE(std::get_if<r::markers::string_t>(&a1.elements[1]) !=
            nullptr);
    REQUIRE(std::get_if<r::markers::string_t>(&a1.elements[2]) !=
            nullptr);

	std::string_view view(ok);
	view = view.substr(54, view.size() - 54);
    parsed_result = r::Protocol::parse(view);
    positive_parse_result = std::get<positive_result_t>(parsed_result);
    REQUIRE(positive_parse_result.consumed);
    REQUIRE(positive_parse_result.consumed == 54);

    auto &a2 = std::get<r::markers::array_holder_t>(
        positive_parse_result.result);
    REQUIRE(a2.elements.size() == 3);
    REQUIRE(std::visit(
        r::marker_helpers::equality("message"), a2.elements[0]));
    REQUIRE(std::visit(
        r::marker_helpers::equality("some-channel1"),
        a2.elements[1]));
    REQUIRE(std::visit(
        r::marker_helpers::equality("message-a2"), a2.elements[2]));
    REQUIRE(std::get_if<r::markers::string_t>(&a2.elements[0]) !=
            nullptr);
    REQUIRE(std::get_if<r::markers::string_t>(&a2.elements[1]) !=
            nullptr);
    REQUIRE(std::get_if<r::markers::string_t>(&a2.elements[2]) !=
            nullptr);

	view = view.substr(54, view.size() - 54);
    parsed_result = r::Protocol::parse(view);
    positive_parse_result = std::get<positive_result_t>(parsed_result);
    REQUIRE(positive_parse_result.consumed);
    REQUIRE(positive_parse_result.consumed == 47);

    auto &a3 = std::get<r::markers::array_holder_t>(
        positive_parse_result.result);
    REQUIRE(a3.elements.size() == 3);
    REQUIRE(std::visit(
        r::marker_helpers::equality("message"), a3.elements[0]));
    REQUIRE(std::visit(
        r::marker_helpers::equality("some-channel2"),
        a3.elements[1]));
    REQUIRE(std::visit(r::marker_helpers::equality("last"),
                                 a3.elements[2]));
    REQUIRE(std::get_if<r::markers::string_t>(&a3.elements[0]) !=
            nullptr);
    REQUIRE(std::get_if<r::markers::string_t>(&a3.elements[1]) !=
            nullptr);
    REQUIRE(std::get_if<r::markers::string_t>(&a3.elements[2]) !=
            nullptr);
}

TEST_CASE("serialize", "[protocol]") {
    std::stringstream buff;
    r::single_command_t cmd("LLEN", "fmm.cheap-travles2");
    r::Protocol::serialize(buff, cmd);
    std::string expected("*2\r\n$4\r\nLLEN\r\n$18\r\nfmm.cheap-travles2\r\n");
    REQUIRE(buff.str() == expected);
};
