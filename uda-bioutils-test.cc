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
  double expected_agreement = 0.25;

  // Run the tests
  if (!logfold_agreement.Execute(vals2, expected_agreement)) {
    cerr << "Logfold agreement: " << logfold_agreement.GetErrorMsg() << endl;
    return false;
  }

  vals2 = {-2,1};
  expected_agreement = -1;

  // Run the tests
  if (!logfold_agreement.Execute(vals2, expected_agreement)) {
    cerr << "Logfold agreement: " << logfold_agreement.GetErrorMsg() << endl;
    return false;
  }

  vals2 = {1,1,1,1,1,2,2};
  expected_agreement = 0.7142857;

  // Run the tests
  if (!logfold_agreement.Execute(vals2, expected_agreement)) {
    cerr << "Logfold agreement: " << logfold_agreement.GetErrorMsg() << endl;
    return false;
  }

  return true;
}

int main(int argc, char** argv) {
  bool passed = true;
  passed &= TestAgreement();
  cerr << (passed ? "Tests passed." : "Tests failed.") << endl;
  return 0;
}
