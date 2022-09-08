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

#include "udf-sero.h"
#include <impala_udf/udf-test-harness.h>
#include <regex>

using namespace impala;
using namespace impala_udf;
using namespace std;

int main(int argc, char **argv)
{
    bool passed = true;


    try {
        passed &= UdfTestHarness::ValidateUdf<StringVal, StringVal, StringVal>(
            Sort_Cohorts, StringVal("Adult, Adult 5-10, Pediatric, Elderly"), StringVal(", "),
            StringVal("Pediatric, Adult, Adult 5-10 and Elderly"));

    } catch (const std::regex_error &e) {
        if (e.code() == std::regex_constants::error_escape) {
            std::cout << "The code was an escape\n";
        }
        std::cout << "regex_error caught: " << e.what() << " " << e.code() << "<<<\n";
    }


    cout << "Tests " << (passed ? "Passed." : "Failed.") << endl;
    return !passed;
}
