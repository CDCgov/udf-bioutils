// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <iostream>
#include <math.h>

#include <impala_udf/uda-test-harness.h>
#include <impala_udf/udf.h>
#include "uda-bioutils.h"

using namespace impala;
using namespace impala_udf;
using namespace std;


// For algorithms that work on floating point values, the results might not match
// exactly due to floating point inprecision. The test harness allows passing a
// custom equality comparator. Here's an example of one that can tolerate some small
// error.
// This is particularly true  for distributed execution since the order the values
// are processed is variable.
bool FuzzyCompare(const DoubleVal& x, const DoubleVal& y) {
  if (x.is_null && y.is_null) return true;
  if (x.is_null || y.is_null) return false;
//  cerr << "Expected: " << y.val << ", Got: " << x.val << endl;
  return fabs(x.val - y.val) < 0.00001;
}



bool TestAgreement() {
  typedef UdaTestHarness<DoubleVal, StringVal, BigIntVal> TestHarness;
  TestHarness logfold_agreement(BoundedArrayInit, BoundedArrayUpdate, BoundedArrayMerge,
				StringStructSerialize, AgreementFinalize);
  logfold_agreement.SetResultComparator(FuzzyCompare);


  // Test empty input
  vector<BigIntVal> vals;
  if (!logfold_agreement.Execute(vals, DoubleVal::null())) {
    cerr << "LOGFOLD AGREEMENT: " << logfold_agreement.GetErrorMsg() << endl;
    return false;
  }


  // Initialize the test values.
  vector<BigIntVal> vals2 = {1,1,2,2,2,2,3,3};
  double expected_agreement = 0.8333333;

  // Run the tests
  if (!logfold_agreement.Execute(vals2, expected_agreement)) {
    cerr << "Logfold agreement: " << logfold_agreement.GetErrorMsg() << endl;
    return false;
  }

  vals2 = {-100,100};
  expected_agreement = -1;

  // Run the tests
  if (!logfold_agreement.Execute(vals2, expected_agreement)) {
    cerr << "Logfold agreement: " << logfold_agreement.GetErrorMsg() << endl;
    return false;
  }

  vals2 = {-2,1};
  expected_agreement = 0.4166667;

  // Run the tests
  if (!logfold_agreement.Execute(vals2, expected_agreement)) {
    cerr << "Logfold agreement: " << logfold_agreement.GetErrorMsg() << endl;
    return false;
  }

  vals2 = {1,1,12,13,14,16};
  expected_agreement = -0.3611111;

  // Run the tests
  if (!logfold_agreement.Execute(vals2, expected_agreement)) {
    cerr << "Logfold agreement: " << logfold_agreement.GetErrorMsg() << endl;
    return false;
  }


  vals2 = {1,1,1,1,1,2,2};
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
  TestHarness variance(	RunningMomentInit, 
			RunningMomentUpdate, 
			RunningMomentMerge,
			StringStructSerialize,
			RunningMomentPopulationVarianceFinalize
			);
  variance.SetResultComparator(FuzzyCompare);

  // Test empty input
  vector<BigIntVal> vals;
  if ( ! variance.Execute(vals, DoubleVal::null()) ) {
    cerr << "Simple variance: " << variance.GetErrorMsg() << endl;
    return false;
  }

  // Initialize the test values.
  int sum = 0;
  for (int i = 0; i < 1001; ++i) {
    vals.push_back(BigIntVal(i));
    sum += i;
  }

  double mean = sum / vals.size();
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
  			BitwiseOrInit, 
			BitwiseOrUpdateMerge, 
			BitwiseOrUpdateMerge,
			NULL,
			BitwiseOrFinalize
			);

  // Test empty input
  vector<BigIntVal> vals;

  if ( ! bw_or.Execute(vals, BigIntVal::null()) ) {
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
int main(int argc, char** argv) {
  bool passed = true;
  passed &= TestAgreement();
  passed &= TestVariance();
  passed &= TestBitwiseOr();
  cerr << (passed ? "Tests passed." : "Tests failed.") << endl;
  return 0;
}
