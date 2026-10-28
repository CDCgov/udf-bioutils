// Samuel S. Shepard, CDC
// Impala user-defined functions for mathematical utilities.

#include "udf-mathutils.h"

#include <boost/math/distributions/normal.hpp>
#include <boost/math/distributions/students_t.hpp>
#include <cmath>
#include <exception>
using namespace boost::math;
using namespace std;

// Quantile function for t-statistic generation
IMPALA_UDF_EXPORT
DoubleVal qt(
    FunctionContext *context, const DoubleVal &confidence, const BigIntVal &sample_size,
    const BooleanVal &two_tailed
) {
    if (confidence.is_null || two_tailed.is_null || sample_size.is_null) {
        return DoubleVal::null();
    }

    // Infinite quantiles and non-finite inputs are represented as SQL NULL.
    // Two-tailed confidence zero is valid: its quantile is the median (zero).
    if (!std::isfinite(confidence.val) || confidence.val >= 1 || confidence.val < 0 ||
        (confidence.val == 0 && !two_tailed.val) || sample_size.val < 2) {
        return DoubleVal::null();
    }

    int64_t Sn = sample_size.val;
    double T   = 0;

    // Rounding errors occur past this point. Since the T distribution converges toward the normal
    // distribution as N grows large, we can switch to using the normal distribution without much of
    // a difference in the statistic.
    try {
        if (Sn > 1404454273) {
            normal dist(0.0, 1.0);
            if (two_tailed.val) {
                T = quantile(complement(dist, (1 - confidence.val) / 2.0));
            } else {
                T = quantile(dist, confidence.val);
            }
        } else {
            students_t dist(Sn - 1);
            if (two_tailed.val) {
                T = quantile(complement(dist, (1 - confidence.val) / 2.0));
            } else {
                T = quantile(dist, confidence.val);
            }
        }
    } catch (const std::exception &) {
        // Boost may also overflow for finite probabilities very near an endpoint.
        return DoubleVal::null();
    }

    return std::isfinite(T) ? DoubleVal(T) : DoubleVal::null();
}

// Confidence interval function. Normally we take alpha but I think backwards, so we take the
// confidence level instead.
IMPALA_UDF_EXPORT
DoubleVal ci_t(
    FunctionContext *context, const DoubleVal &confidence, const BigIntVal &sample_size,
    const DoubleVal &sample_std, const BooleanVal &two_tailed
) {
    if (sample_std.is_null || !std::isfinite(sample_std.val) || sample_std.val < 0) {
        return DoubleVal::null();
    }

    const DoubleVal statistic = qt(context, confidence, sample_size, two_tailed);
    if (statistic.is_null) {
        return DoubleVal::null();
    }

    const double interval =
        statistic.val * sample_std.val / std::sqrt(static_cast<double>(sample_size.val));
    return std::isfinite(interval) ? DoubleVal(interval) : DoubleVal::null();
}


// COnfidence interval function, but we hard-code the two-sided case for function overloading.
IMPALA_UDF_EXPORT
DoubleVal ci_t_twoSided(
    FunctionContext *context, const DoubleVal &confidence, const BigIntVal &sample_size,
    const DoubleVal &sample_std
) {
    return ci_t(context, confidence, sample_size, sample_std, BooleanVal(true));
}
