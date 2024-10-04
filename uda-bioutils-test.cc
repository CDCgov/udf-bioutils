// Samuel S. Shepard, CDC

#include <iostream>
#include <math.h>

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
    bool passing = true;

    vector<StringVal> vals;
    vals.push_back(StringVal("ATG"));
    vals.push_back(StringVal("GGG"));
    vals.push_back(StringVal("TCt"));
    vals.push_back(StringVal("TCT"));

    if (!test.Execute<StringVal>(vals, DoubleVal(1.5))) {
        cerr << "Calculate AA entropy: " << test.GetErrorMsg() << endl;
        passing = false;
    }
    return passing;
}

int main(int argc, char **argv) {
    bool passed = true;
    passed &= TestAgreement();
    passed &= TestVariance();
    passed &= TestBitwiseOr();
    passed &= TestCharEntropy();
    passed &= TestNTEntropy();
    passed &= TestAAEntropy();
    passed &= TestCDEntropy();
    cerr << (passed ? "Tests passed." : "Tests failed.") << endl;
    return 0;
}
