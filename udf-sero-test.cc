// Samuel S. Shepard, CDC

#include <iostream>

#include "udf-sero.h"
#include <impala_udf/udf-test-harness.h>
#include <regex>

using namespace impala;
using namespace impala_udf;
using namespace std;

int main(int argc, char **argv) {
    bool passed = true;

    try {
        passed &= UdfTestHarness::ValidateUdf<StringVal, StringVal, StringVal>(
            Sort_Cohorts, StringVal("Adult, Adult 5-10, Pediatric, Elderly"), StringVal(", "),
            StringVal("Pediatric, Adult, Adult 5-10 and Elderly")
        );

    } catch (const std::regex_error &e) {
        if (e.code() == std::regex_constants::error_escape) {
            std::cout << "The code was an escape\n";
        }
        std::cout << "regex_error caught: " << e.what() << " " << e.code() << "<<<\n";
    }


    cout << "Tests " << (passed ? "Passed." : "Failed.") << endl;
    return !passed;
}
