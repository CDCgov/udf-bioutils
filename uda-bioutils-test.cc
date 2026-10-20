// Samuel S. Shepard, CDC

#include <iostream>
#include <array>
#include <limits>
#include <math.h>
#include <string>

#include "uda-bioutils.h"
#include <impala_udf/uda-test-harness.h>
#include <impala_udf/udf.h>

using namespace impala;
using namespace impala_udf;
using namespace std;

// A minor rewrite of the FuzzyCompare function from the Cloudera examples.
bool FuzzyCompare(const DoubleVal &x, const DoubleVal &y) {
    const double precision = 1.0E-7;

    if (!(x.is_null || y.is_null)) {
        return std::fabs(x.val - y.val) <= precision;
    } else {
        return x.is_null == y.is_null;
    }
}


bool TestAgreement() {
    typedef UdaTestHarness<DoubleVal, StringVal, BigIntVal> TestHarness;
    TestHarness logfold_agreement(
        BoundedArrayInit, BoundedArrayUpdate, BoundedArrayMerge, StringStructSerialize,
        AgreementFinalize
    );
    logfold_agreement.SetResultComparator(FuzzyCompare);


    // Test empty input
    vector<BigIntVal> vals;
    if (!logfold_agreement.Execute(vals, DoubleVal::null())) {
        cerr << "LOGFOLD AGREEMENT: " << logfold_agreement.GetErrorMsg() << endl;
        return false;
    }

    // Initialize the test values.
    vector<BigIntVal> vals2   = {1, 1, 2, 2, 2, 2, 3, 3};
    double expected_agreement = 0.8333333;

    // Run the tests
    if (!logfold_agreement.Execute(vals2, expected_agreement)) {
        cerr << "Logfold agreement: " << logfold_agreement.GetErrorMsg() << endl;
        return false;
    }

    vals2              = {-100, 100};
    expected_agreement = -1;

    // Run the tests
    if (!logfold_agreement.Execute(vals2, expected_agreement)) {
        cerr << "Logfold agreement: " << logfold_agreement.GetErrorMsg() << endl;
        return false;
    }

    vals2              = {-2, 1};
    expected_agreement = 0.4166667;

    // Run the tests
    if (!logfold_agreement.Execute(vals2, expected_agreement)) {
        cerr << "Logfold agreement: " << logfold_agreement.GetErrorMsg() << endl;
        return false;
    }

    vals2              = {1, 1, 12, 13, 14, 16};
    expected_agreement = -0.3611111;

    // Run the tests
    if (!logfold_agreement.Execute(vals2, expected_agreement)) {
        cerr << "Logfold agreement: " << logfold_agreement.GetErrorMsg() << endl;
        return false;
    }


    vals2              = {1, 1, 1, 1, 1, 2, 2};
    expected_agreement = 0.9365079;

    // Run the tests
    if (!logfold_agreement.Execute(vals2, expected_agreement)) {
        cerr << "Logfold agreement: " << logfold_agreement.GetErrorMsg() << endl;
        return false;
    }

    return true;
}

bool TestVariance() {
    // Setup the test UDAs.
    // Note: reinterpret_cast is required because pre-2.9 UDF headers had a spurious "const"
    // specifier in the return type for SerializeFn.
    typedef UdaTestHarness<DoubleVal, StringVal, BigIntVal> TestHarness;
    TestHarness variance(
        RunningMomentInit, RunningMomentUpdate, RunningMomentMerge, StringStructSerialize,
        RunningMomentPopulationVarianceFinalize
    );
    variance.SetResultComparator(FuzzyCompare);

    // Test empty input
    vector<BigIntVal> vals;
    if (!variance.Execute(vals, DoubleVal::null())) {
        cerr << "Simple variance: " << variance.GetErrorMsg() << endl;
        return false;
    }

    // Initialize the test values.
    int sum = 0;
    for (int i = 0; i < 1001; ++i) {
        vals.push_back(BigIntVal(i));
        sum += i;
    }

    double mean              = sum / vals.size();
    double expected_variance = 0;

    for (int i = 0; i < vals.size(); ++i) {
        double d = mean - vals[i].val;
        expected_variance += d * d;
    }

    expected_variance /= static_cast<double>(vals.size());


    // Run the tests
    if (!variance.Execute(vals, expected_variance)) {
        cerr << "Simple variance: " << variance.GetErrorMsg() << endl;
        return false;
    }

    return true;
}

template <typename Input>
bool TestHigherMoments() {
    using TestHarness = UdaTestHarness<DoubleVal, StringVal, Input>;
    TestHarness skewness(
        RunningMomentInit, RunningMomentUpdate, RunningMomentMerge, StringStructSerialize,
        RunningMomentSkewnessFinalize
    );
    TestHarness kurtosis(
        RunningMomentInit, RunningMomentUpdate, RunningMomentMerge, StringStructSerialize,
        RunningMomentKurtosisFinalize
    );
    skewness.SetResultComparator(FuzzyCompare);
    kurtosis.SetResultComparator(FuzzyCompare);

    // ALL exercises Init/Update/Serialize and Init/Merge/Serialize/Finalize, including
    // empty partial states. Closing the harness contexts detects missing state cleanup.
    const vector<Input> empty;
    const vector<Input> values = {Input(1), Input(2), Input(3), Input(4), Input::null()};
    if (!skewness.Execute(empty, DoubleVal::null()) ||
        !skewness.Execute(values, DoubleVal(0))) {
        cerr << "Skewness: " << skewness.GetErrorMsg() << endl;
        return false;
    }
    if (!kurtosis.Execute(empty, DoubleVal::null()) ||
        !kurtosis.Execute(values, DoubleVal(-1.2))) {
        cerr << "Kurtosis: " << kurtosis.GetErrorMsg() << endl;
        return false;
    }
    return true;
}

bool TestBitwiseOr() {
    UdaTestHarness<BigIntVal, BigIntVal, BigIntVal> bw_or(
        BitwiseOrInit, BitwiseOrUpdateMerge, BitwiseOrUpdateMerge, NULL, BitwiseOrFinalize
    );

    // Test empty input
    vector<BigIntVal> vals;

    if (!bw_or.Execute(vals, BigIntVal::null())) {
        //  if ( ! bw_or.Execute(vals, BigIntVal(0) ) ) {
        cerr << "Empty set for bitwise or: " << bw_or.GetErrorMsg() << endl;
        return false;
    }

    vals.push_back(BigIntVal(3));
    vals.push_back(BigIntVal(3));
    vals.push_back(BigIntVal(3));
    vals.push_back(BigIntVal(9));

    // Run the tests
    if (!bw_or.Execute(vals, BigIntVal(11))) {
        cerr << "Bitwise Or (3,3,3,9): " << bw_or.GetErrorMsg() << endl;
        return false;
    }

    vals.push_back(BigIntVal(4));

    // Run the tests
    if (!bw_or.Execute(vals, BigIntVal(15))) {
        cerr << "Bitwise Or (3,3,3,9,4): " << bw_or.GetErrorMsg() << endl;
        return false;
    }

    return true;
}

bool TestCharEntropy() {
    typedef UdaTestHarness<DoubleVal, StringVal, StringVal> TestHarness;
    TestHarness test(
        CalcCharEntropyInit, CalcCharEntropyUpdate, CalcCharEntropyMerge,
        reinterpret_cast<TestHarness::SerializeFn>(CalcCharEntropySerialize),
        CalcCharEntropyFinalize
    );
    // test.SetResultComparator(FuzzyCompareStrings);
    bool passing = true;

    vector<StringVal> vals;
    vals.push_back(StringVal("C"));
    vals.push_back(StringVal("A"));
    vals.push_back(StringVal("Ab"));

    if (!test.Execute<StringVal>(vals, DoubleVal(1.5))) {
        cerr << "Calculate str entropy: " << test.GetErrorMsg() << endl;
        passing = false;
    }
    return passing;
}

bool TestNTEntropy() {
    typedef UdaTestHarness<DoubleVal, StringVal, StringVal> TestHarness;
    TestHarness test(
        CalcNTEntropyInit, CalcNTEntropyUpdate, CalcNTEntropyMerge,
        reinterpret_cast<TestHarness::SerializeFn>(CalcNTEntropySerialize), CalcNTEntropyFinalize
    );
    bool passing = true;

    vector<StringVal> vals;
    vals.push_back(StringVal("C"));
    vals.push_back(StringVal("t"));
    vals.push_back(StringVal("a"));
    vals.push_back(StringVal("A"));

    if (!test.Execute<StringVal>(vals, DoubleVal(1.5))) {
        cerr << "Calculate NT entropy: " << test.GetErrorMsg() << endl;
        passing = false;
    }
    return passing;
}

bool TestAAEntropy() {
    typedef UdaTestHarness<DoubleVal, StringVal, StringVal> TestHarness;
    TestHarness test(
        CalcAAEntropyInit, CalcAAEntropyUpdate, CalcAAEntropyMerge,
        reinterpret_cast<TestHarness::SerializeFn>(CalcAAEntropySerialize), CalcAAEntropyFinalize
    );
    bool passing = true;

    vector<StringVal> vals;
    vals.push_back(StringVal("T"));
    vals.push_back(StringVal("S"));
    vals.push_back(StringVal("D"));
    vals.push_back(StringVal("d"));

    if (!test.Execute<StringVal>(vals, DoubleVal(1.5))) {
        cerr << "Calculate AA entropy: " << test.GetErrorMsg() << endl;
        passing = false;
    }
    return passing;
}

bool TestCDEntropy() {
    typedef UdaTestHarness<DoubleVal, StringVal, StringVal> TestHarness;
    TestHarness test(
        CalcCDEntropyInit, CalcCDEntropyUpdate, CalcCDEntropyMerge,
        reinterpret_cast<TestHarness::SerializeFn>(CalcCDEntropySerialize), CalcCDEntropyFinalize
    );
    test.SetResultComparator(FuzzyCompare);
    bool passing = true;

    vector<StringVal> vals;
    vals.push_back(StringVal("ATG"));
    vals.push_back(StringVal("GGG"));
    vals.push_back(StringVal("TCt"));
    vals.push_back(StringVal("TCT"));

    if (!test.Execute<StringVal>(vals, DoubleVal(1.5))) {
        cerr << "Calculate codon entropy: " << test.GetErrorMsg() << endl;
        passing = false;
    }

    // ACA has index 4, which used to collide with the invalid-codon sentinel.
    vals = {StringVal("AAA"), StringVal("ACA"), StringVal("aaa"), StringVal("aca")};
    if (!test.Execute(vals, DoubleVal(1))) {
        cerr << "Codon entropy including ACA: " << test.GetErrorMsg() << endl;
        passing = false;
    }

    // Equal frequencies of all 64 canonical codons have log2(64) == 6 bits of entropy.
    // Keep the character buffers alive while the distributed harness reads the StringVals.
    array<array<uint8_t, 3>, 64> codons;
    vals.clear();
    size_t index = 0;
    for (uint8_t first : string("ACGT")) {
        for (uint8_t second : string("ACGT")) {
            for (uint8_t third : string("ACGT")) {
                codons[index] = {first, second, third};
                vals.emplace_back(codons[index].data(), 3);
                index++;
            }
        }
    }
    if (!test.Execute(vals, DoubleVal(6))) {
        cerr << "Codon entropy across all 64 codons: " << test.GetErrorMsg() << endl;
        passing = false;
    }

    // Reject invalid bases in each position without dropping valid ACA observations.
    vals = {StringVal("AAA"), StringVal("ACA"), StringVal("NAA"), StringVal("ANA"),
            StringVal("AAN")};
    if (!test.Execute(vals, DoubleVal(1), SINGLE_NODE)) {
        cerr << "Codon entropy with invalid codons: " << test.GetErrorMsg() << endl;
        passing = false;
    }
    return passing;
}

bool TestEntropyCounterLimits() {
    using TestHarness = UdaTestHarness<DoubleVal, StringVal, StringVal>;
    struct EntropyFunctions {
        const char *name;
        TestHarness::InitFn init;
        TestHarness::UpdateFn update;
        TestHarness::MergeFn merge;
        TestHarness::FinalizeFn finalize;
        const char *input;
        size_t counter_index;
    };
    const EntropyFunctions functions[] = {
        {"Character", CalcCharEntropyInit, CalcCharEntropyUpdate, CalcCharEntropyMerge,
         CalcCharEntropyFinalize, "A", 'A'},
        {"Nucleotide", CalcNTEntropyInit, CalcNTEntropyUpdate, CalcNTEntropyMerge,
         CalcNTEntropyFinalize, "A", 1},
        {"Amino acid", CalcAAEntropyInit, CalcAAEntropyUpdate, CalcAAEntropyMerge,
         CalcAAEntropyFinalize, "A", 1},
        {"Codon", CalcCDEntropyInit, CalcCDEntropyUpdate, CalcCDEntropyMerge,
         CalcCDEntropyFinalize, "ACA", 4}
    };
    const auto maximum = std::numeric_limits<unsigned int>::max();
    bool passed = true;
    for (const auto &fn : functions) {
        // Seed states near the serialized counter limit to test billions of observations
        // without billions of Update calls. The intermediate format remains unchanged.
        for (bool merge : {false, true}) {
            FunctionContext *context = UdfTestHarness::CreateTestContext(
                FunctionContext::TypeDesc{}, {}
            );
            StringVal dst;
            StringVal src;
            fn.init(context, &dst);
            fn.init(context, &src);
            auto *dst_counts = reinterpret_cast<unsigned int *>(dst.ptr);
            auto *src_counts = reinterpret_cast<unsigned int *>(src.ptr);
            dst_counts[fn.counter_index] = maximum - 1;
            src_counts[fn.counter_index] = 1;

            auto add_one = [&]() {
                if (merge) fn.merge(context, src, &dst);
                else fn.update(context, StringVal(fn.input), &dst);
            };
            add_one();
            bool exact_limit_ok = !context->has_error() &&
                                  dst_counts[fn.counter_index] == maximum;
            // Update uses plain unsigned increments. Only Merge rejects overflow.
            bool overflow_detected = true;
            if (merge) {
                add_one();
                overflow_detected = context->has_error() &&
                    string(context->error_msg()).find("Entropy counter overflow") != string::npos &&
                    dst_counts[fn.counter_index] == maximum;
            }

            fn.finalize(context, src);
            fn.finalize(context, dst);
            UdfTestHarness::CloseContext(context);
            delete context;
            if (!exact_limit_ok || !overflow_detected) {
                cerr << fn.name << " entropy " << (merge ? "merge" : "update")
                     << " counter limit test failed." << endl;
                passed = false;
            }
        }
    }
    return passed;
}

bool TestPearsonCorrelation() {
    typedef UdaTestHarness2<DoubleVal, StringVal, DoubleVal, DoubleVal> TestHarnessPearson;
    TestHarnessPearson pearson(
        PearsonCorrInit, PearsonCorrUpdate, PearsonCorrMerge,
        reinterpret_cast<TestHarnessPearson::SerializeFn>(PearsonCorrSerialize), PearsonCorrFinalize
    );
    pearson.SetResultComparator(FuzzyCompare);
    {
        // Test all empty
        vector<DoubleVal> x_vals {};
        vector<DoubleVal> y_vals {};
        if (!pearson.Execute(x_vals, y_vals, DoubleVal::null())) {
            cerr << "Pearson Correlation: Empty input test. " << pearson.GetErrorMsg() << endl;
            return false;
        }
    }
    {
        // Test all null
        vector<DoubleVal> x_vals {DoubleVal::null(), DoubleVal::null()};
        vector<DoubleVal> y_vals {DoubleVal::null(), DoubleVal::null()};
        if (!pearson.Execute(x_vals, y_vals, DoubleVal::null())) {
            cerr << "Pearson Correlation: Empty input test. " << pearson.GetErrorMsg() << endl;
            return false;
        }
    }
    {
        // Test empty column
        vector<DoubleVal> x_vals {DoubleVal::null(), DoubleVal::null(), DoubleVal::null()};
        vector<DoubleVal> y_vals {1.0, 2.0, 3.0};
        if (!pearson.Execute(x_vals, y_vals, DoubleVal::null())) {
            cerr << "Pearson Correlation: Empty column test. " << pearson.GetErrorMsg() << endl;
            return false;
        }
        if (!pearson.Execute(y_vals, x_vals, DoubleVal::null())) {
            cerr << "Pearson Correlation: Empty column test. " << pearson.GetErrorMsg() << endl;
            return false;
        }
    }
    {
        // Test too few input
        vector<DoubleVal> x_vals {1.0};
        vector<DoubleVal> y_vals {2.0};
        if (!pearson.Execute(x_vals, y_vals, DoubleVal::null())) {
            cerr << "Pearson Correlation: Too few input test. " << pearson.GetErrorMsg() << endl;
            return false;
        }
    }
    {
        // Minimum input
        vector<DoubleVal> x_vals {1.0, 2.0};
        vector<DoubleVal> y_vals {3.0, 4.0};
        if (!pearson.Execute(x_vals, y_vals, DoubleVal(1.0))) {
            cerr << "Pearson Correlation: Minimum input test. " << pearson.GetErrorMsg() << endl;
            return false;
        }
    }
    {
        // Test zero variance
        vector<DoubleVal> x_vals {5.0, 5.0, 5.0, 5.0};
        vector<DoubleVal> y_vals {1.0, 2.0, 3.0, 4.0};
        if (!pearson.Execute(x_vals, y_vals, DoubleVal::null())) {
            cerr << "Pearson Correlation: Zero Variance test. " << pearson.GetErrorMsg() << endl;
            return false;
        }
        if (!pearson.Execute(y_vals, x_vals, DoubleVal::null())) {
            cerr << "Pearson Correlation: Zero Variance test. " << pearson.GetErrorMsg() << endl;
            return false;
        }
    }
    {
        // Test null pair
        vector<DoubleVal> x_vals {1.0, DoubleVal::null(), 3.0};
        vector<DoubleVal> y_vals {1.0, 999.0, 3.0};
        if (!pearson.Execute(x_vals, y_vals, DoubleVal(1.0))) {
            cerr << "Pearson Correlation: Null row test. " << pearson.GetErrorMsg() << endl;
            return false;
        }
        if (!pearson.Execute(y_vals, x_vals, DoubleVal(1.0))) {
            cerr << "Pearson Correlation: Null row test. " << pearson.GetErrorMsg() << endl;
            return false;
        }
    }
    {
        // Test infinity pair
        vector<DoubleVal> x_vals {1.0, std::numeric_limits<double>::infinity(), 3.0};
        vector<DoubleVal> y_vals {1.0, 999.0, 3.0};
        if (!pearson.Execute(x_vals, y_vals, DoubleVal(1.0))) {
            cerr << "Pearson Correlation: Infinite row test. " << pearson.GetErrorMsg() << endl;
            return false;
        }
        if (!pearson.Execute(y_vals, x_vals, DoubleVal(1.0))) {
            cerr << "Pearson Correlation: Infinite row test. " << pearson.GetErrorMsg() << endl;
            return false;
        }
    }
    {
        // perfect +1
        vector<DoubleVal> x_vals {1.0, 2.0, 3.0, 4.0};
        vector<DoubleVal> y_vals {1.0, 2.0, 3.0, 4.0};
        if (!pearson.Execute(x_vals, y_vals, DoubleVal(1.0))) {
            cerr << "Pearson Correlation: Perfect +1 test. " << pearson.GetErrorMsg() << endl;
            return false;
        }
    }
    {
        // perfect -1
        vector<DoubleVal> x_vals {1.0, 2.0, 3.0, 4.0};
        vector<DoubleVal> y_vals {4.0, 3.0, 2.0, 1.0};
        if (!pearson.Execute(x_vals, y_vals, DoubleVal(-1.0))) {
            cerr << "Pearson Correlation: Perfect -1 test. " << pearson.GetErrorMsg() << endl;
            return false;
        }
    }
    {
        // Test very large numbers
        vector<DoubleVal> x_vals {1.0e12, 2.0e12, 3.0e12};
        vector<DoubleVal> y_vals {1.0e12, 2.0e12, 3.0e12};
        if (!pearson.Execute(x_vals, y_vals, DoubleVal(1.0))) {
            cerr << "Pearson Correlation: Very large numbers test. " << pearson.GetErrorMsg() << endl;
            return false;
        }
    }
    {
        // Test very small numbers
        vector<DoubleVal> x_vals {1.0e-12, 2.0e-12, 3.0e-12};
        vector<DoubleVal> y_vals {1.0e-12, 2.0e-12, 3.0e-12};
        if (!pearson.Execute(x_vals, y_vals, DoubleVal(1.0))) {
            cerr << "Pearson Correlation: Very small numbers test. " << pearson.GetErrorMsg() << endl;
            return false;
        }
    }
    {
        // Test mixed magnitude
        vector<DoubleVal> x_vals {1.0e12, 2.0e12+1, 3.0e12+2};
        vector<DoubleVal> y_vals {1.0, 2.0, 3.0};
        if (!pearson.Execute(x_vals, y_vals, DoubleVal(1.0))) {
            cerr << "Pearson Correlation: Mixed magnitude test. " << pearson.GetErrorMsg() << endl;
            return false;
        }
    }
    {
        // Test negative numbers
        vector<DoubleVal> x_vals {-1.0, -2.0, -3.0};
        vector<DoubleVal> y_vals {1.0, 2.0, 3.0};
        if (!pearson.Execute(x_vals, y_vals, DoubleVal(-1.0))) {
            cerr << "Pearson Correlation: Negative numbers test. " << pearson.GetErrorMsg() << endl;
            return false;
        }
        if (!pearson.Execute(y_vals, x_vals, DoubleVal(-1.0))) {
            cerr << "Pearson Correlation: Negative numbers test. " << pearson.GetErrorMsg() << endl;
            return false;
        }
    }
    {
        // Test large datasets
        vector<DoubleVal> x_vals;
        vector<DoubleVal> y_vals;
        for (int i=0;i<10000;i++) {
            x_vals.push_back(i);
            y_vals.push_back(i);
        }
        if (!pearson.Execute(x_vals, y_vals, DoubleVal(1.0))) {
            cerr << "Pearson Correlation: Large dataset test. " << pearson.GetErrorMsg() << endl;
            return false;
        }
    }
    {
        // Test random dataset
        vector<DoubleVal> x_vals {1.0, 2.0, 3.0, 4.0, 5.0};
        vector<DoubleVal> y_vals {2.0, 1.0, 4.0, 3.0, 5.0};
        if (!pearson.Execute(x_vals, y_vals, DoubleVal(0.8))) {
            cerr << "Pearson Correlation: Random ties dataset. " << pearson.GetErrorMsg() << endl;
            return false;
        }
    }
    return true;
}

int main(int argc, char **argv) {
    bool passed = true;
    passed &= TestAgreement();
    passed &= TestVariance();
    passed &= TestHigherMoments<BigIntVal>();
    passed &= TestHigherMoments<DoubleVal>();
    passed &= TestBitwiseOr();
    passed &= TestCharEntropy();
    passed &= TestNTEntropy();
    passed &= TestAAEntropy();
    passed &= TestCDEntropy();
    passed &= TestEntropyCounterLimits();
    passed &= TestPearsonCorrelation();
    cerr << (passed ? "Tests passed." : "Tests failed.") << endl;
    return passed ? 0 : 1;
}
