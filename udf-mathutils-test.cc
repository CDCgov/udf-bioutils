// Samuel S. Shepard, CDC

#include "udf-mathutils.h"
#include <impala_udf/udf-test-harness.h>

#include <algorithm>
#include <cmath>
#include <exception>
#include <iomanip>
#include <iostream>
#include <limits>
#include <memory>
#include <vector>

namespace {

bool check_result(const char *name, const DoubleVal &actual, const DoubleVal &expected) {
    const bool equal = actual.is_null == expected.is_null &&
                       (expected.is_null || (std::isfinite(actual.val) &&
                                             std::abs(actual.val - expected.val) <=
                                                 1e-10 * std::max(1.0, std::abs(expected.val))));
    if (!equal) {
        std::cerr << std::setprecision(17) << name << ": expected ";
        if (expected.is_null)
            std::cerr << "NULL";
        else
            std::cerr << expected.val;
        std::cerr << ", got ";
        if (actual.is_null)
            std::cerr << "NULL";
        else
            std::cerr << actual.val;
        std::cerr << '\n';
    }
    return equal;
}

bool test_math(FunctionContext *context) {
    const double nan      = std::numeric_limits<double>::quiet_NaN();
    const double inf      = std::numeric_limits<double>::infinity();
    const double max      = std::numeric_limits<double>::max();
    const double tiny     = std::numeric_limits<double>::denorm_min();
    const int64_t large_n = std::numeric_limits<int64_t>::max();
    const DoubleVal null  = DoubleVal::null();
    bool passed           = true;

    struct QuantileCase {
        const char *name;
        DoubleVal confidence;
        BigIntVal sample_size;
        BooleanVal two_tailed;
        DoubleVal expected;
    };
    const std::vector<QuantileCase> quantiles = {
        {"t two-sided", 0.95, 10, true, 2.262157162798205},
        {"t one-sided", 0.95, 10, false, 1.833112932653633},
        {"minimum sample size", 0.5, 2, true, 1.0},
        {"normal two-sided", 0.95, large_n, true, 1.959963984540054},
        {"normal one-sided", 0.95, large_n, false, 1.6448536269514722},
        {"t median", 0.5, 10, false, 0.0},
        {"zero two-sided confidence", 0.0, 10, true, 0.0},
        {"normal zero two-sided confidence", 0.0, large_n, true, 0.0},
        {"zero one-sided confidence", 0.0, 10, false, null},
        {"unit two-sided confidence", 1.0, 10, true, null},
        {"unit one-sided confidence", 1.0, 10, false, null},
        {"normal unit confidence", 1.0, large_n, true, null},
        {"negative confidence", -0.1, 10, true, null},
        {"excess confidence", 1.1, 10, true, null},
        {"NaN confidence", nan, 10, true, null},
        {"positive infinite confidence", inf, 10, true, null},
        {"negative infinite confidence", -inf, 10, true, null},
        {"unrepresentable t quantile", tiny, 2, false, null},
        {"insufficient samples", 0.95, 1, true, null},
        {"negative samples", 0.95, -1, true, null},
        {"null confidence", null, 10, true, null},
        {"null samples", 0.95, BigIntVal::null(), true, null},
        {"null sidedness", 0.95, 10, BooleanVal::null(), null},
    };
    for (const auto &test : quantiles) {
        passed &= check_result(
            test.name, qt(context, test.confidence, test.sample_size, test.two_tailed),
            test.expected
        );
    }

    struct IntervalCase {
        const char *name;
        DoubleVal confidence;
        BigIntVal sample_size;
        DoubleVal sample_std;
        BooleanVal two_tailed;
        DoubleVal expected;
    };
    const std::vector<IntervalCase> intervals = {
        {"standard interval", 0.95, 10, 2.0, true, 2.0 * 2.262157162798205 / std::sqrt(10.0)},
        {"one-sided interval", 0.95, 10, 2.0, false, 2.0 * 1.833112932653633 / std::sqrt(10.0)},
        {"minimum sample interval", 0.5, 2, 2.0, true, std::sqrt(2.0)},
        {"zero standard deviation", 0.95, 10, 0.0, true, 0.0},
        {"zero interval confidence", 0.0, 10, 2.0, true, 0.0},
        // Invalid quantile inputs must still be rejected when the margin would be zero.
        {"invalid confidence with zero standard deviation", 1.0, 10, 0.0, true, null},
        {"negative standard deviation", 0.95, 10, -1.0, true, null},
        {"NaN standard deviation", 0.95, 10, nan, true, null},
        {"infinite standard deviation", 0.95, 10, inf, true, null},
        {"negative infinite standard deviation", 0.95, 10, -inf, true, null},
        {"unrepresentable interval", 0.95, 2, max, true, null},
        {"interval intermediate product overflow", 0.95, 100, max, true, null},
        {"unrepresentable interval quantile", tiny, 2, 1.0, false, null},
        {"insufficient interval samples", 0.95, 1, 2.0, true, null},
        {"null standard deviation", 0.95, 10, null, true, null},
    };
    for (const auto &test : intervals) {
        passed &= check_result(
            test.name,
            ci_t(context, test.confidence, test.sample_size, test.sample_std, test.two_tailed),
            test.expected
        );
    }

    // The three-argument wrapper defaults to a two-sided interval.
    passed &= check_result(
        "default two-sided interval", ci_t_twoSided(context, 0.95, 10, 2.0),
        2.0 * 2.262157162798205 / std::sqrt(10.0)
    );
    passed &= check_result(
        "default interval rejects invalid confidence", ci_t_twoSided(context, 1.0, 10, 2.0), null
    );
    return passed;
}

} // namespace

int main() {
    FunctionContext::TypeDesc return_type{};
    return_type.type = FunctionContext::TYPE_DOUBLE;
    std::unique_ptr<FunctionContext> context(UdfTestHarness::CreateTestContext(return_type, {}));
    bool passed = false;
    try {
        passed = test_math(context.get());
    } catch (const std::exception &error) {
        std::cerr << "Exception escaped math UDF: " << error.what() << '\n';
    }
    UdfTestHarness::CloseContext(context.get());
    if (context->has_error()) {
        std::cerr << context->error_msg() << '\n';
        passed = false;
    }
    std::cerr << (passed ? "Tests passed." : "Tests failed.") << '\n';
    return passed ? 0 : 1;
}
