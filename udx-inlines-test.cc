#include <algorithm>
#include <iostream>
#include <limits>
#include <string_view>
#include <impala_udf/udf.h>

using namespace impala_udf;
#include "udx-inlines.h"

int main() {
    bool passed = true;
    const auto check = [&passed](bool condition, const char *description) {
        if (!condition) {
            std::cerr << description << '\n';
            passed = false;
        }
    };

    const std::string_view limits = "-2147483648,0,2147483647";
    const std::vector<int> expected = {
        std::numeric_limits<int>::min(), 0, std::numeric_limits<int>::max()
    };
    check(split_int_by_substr(limits, ",") == expected, "Integer limits must parse");
    check(split_unique_sequence_by_substr(limits, ",") == expected,
          "Unique integer limits must parse");
    check(split_ordered_set_by_substr(limits, ",") == std::set<int>(expected.begin(), expected.end()),
          "Ordered integer limits must parse");

    // Overflow must reject the entire input, including any successfully parsed prefix.
    for (const std::string_view input : {
             "2147483648", "-2147483649", "999999999999999999999999999999",
             "1,2147483648,2", "1,-2147483649,2", "1,invalid,2"
         }) {
        check(split_int_by_substr(input, ",").empty(), "Invalid integer list was accepted");
        check(split_unique_sequence_by_substr(input, ",").empty(),
              "Invalid unique integer list was accepted");
        check(split_ordered_set_by_substr(input, ",").empty(),
              "Invalid ordered integer list was accepted");
    }

    check(split_int_by_substr("2,1,2", ",") == std::vector<int>({2, 1, 2}),
          "Integer parser must preserve order and duplicates");
    check(split_unique_sequence_by_substr("2,1,2", ",") == std::vector<int>({1, 2}),
          "Unique parser must sort and deduplicate");
    check(split_ordered_set_by_substr("2,1,2", ",") == std::set<int>({1, 2}),
          "Ordered parser must sort and deduplicate");

    std::cerr << (passed ? "Tests passed." : "Tests failed.") << '\n';
    return passed ? 0 : 1;
}
