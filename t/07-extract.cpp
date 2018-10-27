#include <boost/asio.hpp>
#include <string>

#include "bredis/Extract.hpp"

#include "catch.hpp"

namespace r = bredis;
namespace asio = boost::asio;

using Buffer = asio::const_buffers_1;
using Iterator = boost::asio::buffers_iterator<Buffer, char>;

TEST_CASE("string extraction", "[protocol]") {
    std::string source = "src";
    Buffer buff{source.c_str(), source.size()};
    r::markers::string_t marker{source};
    r::markers::redis_result_t erased_marker{marker};

    auto r = std::visit(r::extractor(), erased_marker);
    auto *target = std::get_if<r::extracts::string_t>(&r);
    REQUIRE(target);
    REQUIRE(target->str == source);
}

TEST_CASE("error extraction", "[protocol]") {
    std::string source = "src";
    Buffer buff{source.c_str(), source.size()};
    r::markers::error_t marker{r::markers::string_t{source}};
    r::markers::redis_result_t erased_marker{marker};

    auto r = std::visit(r::extractor(), erased_marker);
    auto *target = std::get_if<r::extracts::error_t>(&r);
    REQUIRE(target);
    REQUIRE(target->str == source);
}

TEST_CASE("nil extraction", "[protocol]") {
    std::string source = "src";
    Buffer buff{source.c_str(), source.size()};
    r::markers::nil_t marker{r::markers::string_t{source}};
    r::markers::redis_result_t erased_marker{marker};

    auto r = std::visit(r::extractor(), erased_marker);
    auto *target = std::get_if<r::extracts::nil_t>(&r);
    REQUIRE(target);
}

TEST_CASE("simple int extraction", "[protocol]") {
    std::string source = "5";
    Buffer buff{source.c_str(), source.size()};
    r::markers::int_t marker{r::markers::string_t{source}};
    r::markers::redis_result_t erased_marker{marker};

    auto r = std::visit(r::extractor(), erased_marker);
    auto *target = std::get_if<r::extracts::int_t>(&r);
    REQUIRE(target);
    REQUIRE(*target == 5);
}

TEST_CASE("large int extraction", "[protocol]") {
    std::string source = "9223372036854775801";
    Buffer buff{source.c_str(), source.size()};
    r::markers::int_t marker{r::markers::string_t{source}};
    r::markers::redis_result_t erased_marker{marker};

    auto r = std::visit(r::extractor(), erased_marker);
    auto *target = std::get_if<r::extracts::int_t>(&r);
    REQUIRE(target);
    REQUIRE(*target == 9223372036854775801);
}

TEST_CASE("negative int extraction", "[protocol]") {
    std::string source = "-123";
    Buffer buff{source.c_str(), source.size()};
    r::markers::int_t marker{r::markers::string_t{source}};
    r::markers::redis_result_t erased_marker{marker};

    auto r = std::visit(r::extractor(), erased_marker);
    auto *target = std::get_if<r::extracts::int_t>(&r);
    REQUIRE(target);
    REQUIRE(*target == -123);
}

TEST_CASE("NaN int extraction attempt", "[protocol]") {
    std::string source = "-9ab22";
    Buffer buff{source.c_str(), source.size()};
    r::markers::int_t marker{r::markers::string_t{source}};
    r::markers::redis_result_t erased_marker{marker};

    REQUIRE_THROWS(
        std::visit(r::extractor(), erased_marker));
}

TEST_CASE("too large int extraction attempt", "[protocol]") {
    std::string source = "92233720368547758019223372036854775801";
    Buffer buff{source.c_str(), source.size()};
    r::markers::int_t marker{r::markers::string_t{source}};
    r::markers::redis_result_t erased_marker{marker};

    REQUIRE_THROWS(
        std::visit(r::extractor(), erased_marker));
}

TEST_CASE("vector extraction", "[protocol]") {
    std::string source_1 = "src";
    Buffer buff_1{source_1.c_str(), source_1.size()};
    r::markers::string_t marker_1{source_1};

    std::string source_2 = "5";
    Buffer buff_2{source_2.c_str(), source_2.size()};
    r::markers::int_t marker_2{r::markers::string_t{source_2}};
    r::markers::array_holder_t source_vector{{marker_1, marker_2}};
    r::markers::redis_result_t erased_marker{
        r::markers::array_holder_t{{marker_1, marker_2}}};

    auto r = std::visit(r::extractor(), erased_marker);
    auto *target = std::get_if<r::extracts::array_holder_t>(&r);
    REQUIRE(target);
    REQUIRE(target->elements.size() == 2);
    auto *t1 = std::get_if<r::extracts::string_t>(&target->elements[0]);
    auto *t2 = std::get_if<r::extracts::int_t>(&target->elements[1]);
    REQUIRE(t1);
    REQUIRE(t2);
    REQUIRE(t1->str == "src");
    REQUIRE(*t2 == 5);
}
