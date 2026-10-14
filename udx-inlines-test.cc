#include <algorithm>
#include <iostream>
#include <limits>
#include <string_view>
#include <impala_udf/udf.h>

using namespace impala_udf;
#include "udx-inlines.h"

namespace {

bool check(bool condition, const char *description) {
    if (!condition) {
        std::cerr << description << '\n';
    }
    return condition;
}

bool test__integer_parsers_accept_int_limits() {
    bool passed = true;
    const std::string_view limits = "-2147483648,0,2147483647";
    const std::vector<int> expected = {
        std::numeric_limits<int>::min(), 0, std::numeric_limits<int>::max()
    };
    passed &= check(split_int_by_substr(limits, ",") == expected, "Integer limits must parse");
    passed &= check(split_unique_sequence_by_substr(limits, ",") == expected,
                    "Unique integer limits must parse");
    passed &= check(
        split_ordered_set_by_substr(limits, ",") == std::set<int>(expected.begin(), expected.end()),
        "Ordered integer limits must parse");

    return passed;
}

bool test__integer_parsers_reject_invalid_lists() {
    bool passed = true;
    // Overflow must reject the entire input, including any successfully parsed prefix.
    for (const std::string_view input : {
             "2147483648", "-2147483649", "999999999999999999999999999999",
             "1,2147483648,2", "1,-2147483649,2", "1,invalid,2"
         }) {
        passed &= check(split_int_by_substr(input, ",").empty(), "Invalid integer list was accepted");
        passed &= check(split_unique_sequence_by_substr(input, ",").empty(),
                        "Invalid unique integer list was accepted");
        passed &= check(split_ordered_set_by_substr(input, ",").empty(),
                        "Invalid ordered integer list was accepted");
    }

    return passed;
}

bool test__integer_parsers_order_and_duplicates() {
    bool passed = true;
    passed &= check(split_int_by_substr("2,1,2", ",") == std::vector<int>({2, 1, 2}),
                    "Integer parser must preserve order and duplicates");
    passed &= check(split_unique_sequence_by_substr("2,1,2", ",") == std::vector<int>({1, 2}),
                    "Unique parser must sort and deduplicate");
    passed &= check(split_ordered_set_by_substr("2,1,2", ",") == std::set<int>({1, 2}),
                    "Ordered parser must sort and deduplicate");

    return passed;
}

} // namespace

int main() {
    bool passed = true;
    passed &= test__integer_parsers_accept_int_limits();
    passed &= test__integer_parsers_reject_invalid_lists();
    passed &= test__integer_parsers_order_and_duplicates();

    std::cerr << (passed ? "Tests passed." : "Tests failed.") << '\n';
    return passed ? 0 : 1;
}
