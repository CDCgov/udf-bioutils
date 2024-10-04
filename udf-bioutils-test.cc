// Samuel S. Shepard, CDC

#include <iostream>

#include "udf-bioutils.h"
#include <impala_udf/udf-test-harness.h>
#include <tuple>

using namespace impala;
using namespace impala_udf;
using namespace std;

bool test__any_instr() {
    int passing = true;

    std::tuple<StringVal, StringVal, BooleanVal> table[7] = {
        std::make_tuple("ABCDEFG", "abcxyz", false),
        std::make_tuple("ABCDEFG", "abcCxyz", true),
        std::make_tuple("", "abcCxyz", false),
        std::make_tuple("ABCDEFG", "", false),
        std::make_tuple("", "", true),
        std::make_tuple(StringVal::null(), "abcCxyz", BooleanVal::null()),
        std::make_tuple("ABCDEFG", StringVal::null(), BooleanVal::null())
    };


    for (int i = 0; i < 7; i++) {
        auto [haystack, needles, expected] = table[i];
        if (!UdfTestHarness::ValidateUdf<BooleanVal, StringVal, StringVal>(
                Find_Set_In_String, haystack, needles, expected
            )) {
            cout << "UDX any_instr(SS)->B failed:\n\t|" << haystack.ptr << "|\n\t|" << needles.ptr
                 << "|\n\t|" << expected.val << "|\n";
            passing = false;
        }
    }

    return passing;
}


bool test__complete_date() {
    int passing = true;

    std::tuple<StringVal, StringVal> table[11] = {
        std::make_tuple("2019", "2019-01-01"),
        std::make_tuple("2019-03", "2019-03-01"),
        std::make_tuple("2019-03-15", "2019-03-15"),
        std::make_tuple("STARK", StringVal::null()),
        std::make_tuple("", StringVal::null()),
        std::make_tuple(StringVal::null(), StringVal::null()),
        std::make_tuple("2010.02", "2010-02-01"),
        std::make_tuple("1981.09.12", "1981-09-12"),
        std::make_tuple("2000/01", "2000-01-01"),
        std::make_tuple("1", StringVal::null()),
        std::make_tuple("0000-01-01", "0000-01-01")
    };
    for (int i = 0; i < 11; i++) {
        auto [arg0_s, expected] = table[i];

        if (!UdfTestHarness::ValidateUdf<StringVal, StringVal>(
                Complete_String_Date, arg0_s, expected
            )) {
            cout << "UDX complete_date(s)->s failed:\n\t|" << arg0_s.ptr << "|\n\t|" << expected.ptr
                 << "|\n";
            passing = false;
        }
    }

    return passing;
}

DateVal to_dv(int yr, int mon, int day) {
    boost::gregorian::date d(yr, mon, day);
    return DateVal(d.day_number() - EPOCH_OFFSET);
}

bool test__ending_in_saturday_str() {
    int passing = true;

    std::tuple<StringVal, DateVal> table[11] = {
        std::make_tuple("2019", DateVal::null()),
        std::make_tuple("2019-03", DateVal::null()),
        std::make_tuple("2019-03-15", to_dv(2019, 3, 16)),
        std::make_tuple("STARK", DateVal::null()),
        std::make_tuple("", DateVal::null()),
        std::make_tuple(StringVal::null(), DateVal::null()),
        std::make_tuple("2010.2.1", to_dv(2010, 2, 6)),
        std::make_tuple("1981.09.12", to_dv(1981, 9, 12)),
        std::make_tuple("2000/01/01", to_dv(2000, 1, 1)),
        std::make_tuple("1", DateVal::null()),
        std::make_tuple("0000-01-01", DateVal::null())
    };
    for (int i = 0; i < 11; i++) {
        auto [arg0_s, expected] = table[i];

        if (!UdfTestHarness::ValidateUdf<DateVal, StringVal>(
                Date_Ending_In_Saturday_STR, arg0_s, expected
            )) {
            cout << "UDX ending_in_saturday(s)->s failed:\n\t|" << arg0_s.ptr << "|\n\t|"
                 << expected.val << "|\n";
            passing = false;
        }
    }

    return passing;
}

bool test__ending_in_fornight_str() {
    int passing = true;


    std::tuple<StringVal, BooleanVal, DateVal> table[14] = {
        std::make_tuple("2019", BooleanVal(true), DateVal::null()),
        std::make_tuple("2019-03", BooleanVal(true), DateVal::null()),
        std::make_tuple("2019-03-15", BooleanVal(true), to_dv(2019, 3, 23)),
        std::make_tuple("STARK", BooleanVal(true), DateVal::null()),
        std::make_tuple("", BooleanVal(true), DateVal::null()),
        std::make_tuple("2010.2.1", BooleanVal(true), to_dv(2010, 2, 6)),
        std::make_tuple("1981.09.12", BooleanVal(true), to_dv(1981, 9, 12)),
        std::make_tuple("2000/01/01", BooleanVal(true), to_dv(2000, 1, 8)),
        std::make_tuple("1", BooleanVal(true), DateVal::null()),
        std::make_tuple("0000-01-01", BooleanVal(true), DateVal::null()),
        std::make_tuple(StringVal::null(), BooleanVal(true), DateVal::null()),
        std::make_tuple("2024-04-15", BooleanVal::null(), DateVal::null()),
        std::make_tuple("2024-04-15", BooleanVal(true), to_dv(2024, 4, 27)),
        std::make_tuple("2024-04-15", BooleanVal(false), to_dv(2024, 4, 20))
    };
    for (int i = 0; i < 14; i++) {
        auto [arg0_s, arg1_b, expected] = table[i];

        if (!UdfTestHarness::ValidateUdf<DateVal, StringVal, BooleanVal>(
                Fortnight_Date_Either_STR, arg0_s, arg1_b, expected
            )) {
            cout << "UDX ending_in_saturday(s)->s failed:\n\t|" << arg0_s.ptr << "|\n\t|"
                 << arg1_b.val << "|\n\t|" << expected.val << "|\n";
            passing = false;
        }
    }

    return passing;
}

bool test__ending_in_saturday_date() {
    int passing = true;

    std::tuple<DateVal, DateVal> table[9] = {
        std::make_tuple(to_dv(2019, 1, 1), to_dv(2019, 1, 5)),
        std::make_tuple(to_dv(2019, 3, 1), to_dv(2019, 3, 2)),
        std::make_tuple(to_dv(2019, 3, 15), to_dv(2019, 3, 16)),
        std::make_tuple(DateVal::null(), DateVal::null()),
        std::make_tuple(to_dv(2010, 2, 1), to_dv(2010, 2, 6)),
        std::make_tuple(to_dv(1981, 9, 12), to_dv(1981, 9, 12)),
        std::make_tuple(to_dv(2000, 1, 1), to_dv(2000, 1, 1)),
        std::make_tuple(to_dv(1, 0, 0), DateVal::null()),
        std::make_tuple(to_dv(0, 1, 1), DateVal::null())
    };
    for (int i = 0; i < 0; i++) {
        auto [arg0_s, expected] = table[i];

        if (!UdfTestHarness::ValidateUdf<DateVal, DateVal>(
                Date_Ending_In_Saturday_DATE, arg0_s, expected
            )) {
            cout << "UDX ending_in_saturday(s)->s failed:\n\t|" << arg0_s.val << "|\n\t|"
                 << expected.val << "|\n";
            passing = false;
        }
    }

    return passing;
}


bool test__contains_element() {
    int passing = true;

    std::tuple<StringVal, StringVal, StringVal, BooleanVal> table[12] = {
        std::make_tuple("baby whales", "BABY;WHALES;FISH", ";", false),
        std::make_tuple("baby whales", "baby;whales;fish", ";", true),
        std::make_tuple(StringVal::null(), "whales;baby", ";", BooleanVal::null()),
        std::make_tuple("baby whales", StringVal::null(), ";", BooleanVal::null()),
        std::make_tuple("baby whales", "whales;baby", StringVal::null(), BooleanVal::null()),
        std::make_tuple("", "whales;baby", ";", false),
        std::make_tuple("baby whales", "", ";", false),
        std::make_tuple("baby whales", "xz", "", false),
        std::make_tuple("baby whales", "xyz", "", true),
        std::make_tuple("baby whales", "xYz", "", false),
        std::make_tuple("300028908", "28907::28906::28905", "::", false),
        std::make_tuple("300028908", "28908::28907::28906::28905", "::", true)
    };

    for (int i = 0; i < 12; i++) {
        auto [needle, list, delim, expected] = table[i];
        if (!UdfTestHarness::ValidateUdf<BooleanVal, StringVal, StringVal, StringVal>(
                Contains_An_Element, needle, list, delim, expected
            )) {
            cout << "UDX contains_element(SSS)->B failed:\n\t|" << needle.ptr << "|\n\t|"
                 << list.ptr << "|\n\t|" << delim.ptr << "|\n\t|" << expected.val << "|\n";
            passing = false;
        }
    }

    return passing;
}


bool test__contains_sym() {
    int passing = true;

    std::tuple<StringVal, StringVal, BooleanVal> table[9] = {
        std::make_tuple("sam", "samuel", true),
        std::make_tuple("samuel", "sam", true),
        std::make_tuple(StringVal::null(), "sam", BooleanVal::null()),
        std::make_tuple("sam", StringVal::null(), BooleanVal::null()),
        std::make_tuple("", "sam", false),
        std::make_tuple("sam", "", false),
        std::make_tuple("", "", true),
        std::make_tuple("sam", "sam", true),
        std::make_tuple("SAM", "samuel", false)
    };
    for (int i = 0; i < 9; i++) {
        auto [arg0_s, arg1_s, expected] = table[i];

        if (!UdfTestHarness::ValidateUdf<BooleanVal, StringVal, StringVal>(
                Contains_Symmetric, arg0_s, arg1_s, expected
            )) {
            cout << "UDX contains_sym(ss)->b failed:\n\t|" << arg0_s.ptr << "|\n\t|" << arg1_s.ptr
                 << "|\n\t|" << expected.val << "|\n";
            passing = false;
        }
    }

    return passing;
}

bool test__cut_paste() {
    int passing = true;

    std::tuple<StringVal, StringVal, StringVal, StringVal> table[14] = {
        std::make_tuple("Sam-The-Wham", "-", "1-2", "Sam-The"),
        std::make_tuple("Sam-The-Wham", "-", "3-1", "Wham-The-Sam"),
        std::make_tuple("Sam-The-Wham", "-", "3,2,1", "Wham-The-Sam"),
        std::make_tuple(
            "There is just one problem here", " ", "1,3;5-6", "There just problem here"
        ),
        std::make_tuple("", "-", "1-2", ""),
        std::make_tuple("Sam-The-Wham", "", "1-2", "Sam-The-Wham"),
        std::make_tuple("Sam-The-Wham", "-", "", "Sam-The-Wham"),
        std::make_tuple(StringVal::null(), "-", "1-2", StringVal::null()),
        std::make_tuple("Sam-The-Wham", StringVal::null(), "1-2", StringVal::null()),
        std::make_tuple("Sam-The-Wham", "-", StringVal::null(), StringVal::null()),
        std::make_tuple("Sam-The-Wham", "-", "A-B", StringVal::null()),
        std::make_tuple("Sam-The-Wham", "@", "3,2,1", "Sam-The-Wham"),
        std::make_tuple(
            "The::fields::are::cut::pastable::!", "::", "1..3;6,6;4-3",
            "The::fields::are::!::!::cut::are"
        ),
        std::make_tuple("Sam-The-Wham", "-", "A-B;a,b,c", StringVal::null())


    };
    for (int i = 0; i < 14; i++) {
        auto [arg0_s, arg1_s, arg2_s, expected] = table[i];

        if (!UdfTestHarness::ValidateUdf<StringVal, StringVal, StringVal, StringVal>(
                Cut_Paste, arg0_s, arg1_s, arg2_s, expected
            )) {
            cout << "UDX cut_paste(sss)->s failed:\n\t|" << arg0_s.ptr << "|\n\t|" << arg1_s.ptr
                 << "|\n\t|" << arg2_s.ptr << "|\n\t|" << expected.ptr << "|\n";
            passing = false;
        }
    }

    return passing;
}

bool test__cut_paste_out() {
    int passing = true;

    std::tuple<StringVal, StringVal, StringVal, StringVal, StringVal> table[16] = {
        std::make_tuple("Sam-The-Wham", "-", "1-2", "/", "Sam/The"),
        std::make_tuple("Sam-The-Wham", "-", "3-1", "/", "Wham/The/Sam"),
        std::make_tuple("Sam-The-Wham", "-", "3,2,1", "//", "Wham//The//Sam"),
        std::make_tuple(
            "There is just one problem here", " ", "1,3;5-6", "", "Therejustproblemhere"
        ),
        std::make_tuple("", "-", "1-2", "/", ""),
        std::make_tuple("Sam-The-Wham", "", "1-2", "/", "Sam-The-Wham"),
        std::make_tuple("Sam-The-Wham", "-", "", "/", "Sam-The-Wham"),
        std::make_tuple("Sam-The-Wham", "-", "1-2", "", "SamThe"),
        std::make_tuple(StringVal::null(), "-", "1-2", "/", StringVal::null()),
        std::make_tuple("Sam-The-Wham", StringVal::null(), "1-2", "/", StringVal::null()),
        std::make_tuple("Sam-The-Wham", "-", StringVal::null(), "/", StringVal::null()),
        std::make_tuple("Sam-The-Wham", "-", "A-B", ",", StringVal::null()),
        std::make_tuple("Sam-The-Wham", "-", "1-2", StringVal::null(), "Sam-The"),
        std::make_tuple("Sam-The-Wham", "@", "3,2,1", "/", "Sam-The-Wham"),
        std::make_tuple(
            "The::fields::are::cut::pastable::!", "::", "1..3;6,6;4-3", " ",
            "The fields are ! ! cut are"
        ),
        std::make_tuple("Sam-The-Wham", "-", "A-B;a,b,c", "/", StringVal::null())
    };

    for (int i = 0; i < 16; i++) {
        auto [arg0_s, arg1_s, arg2_s, arg3_s, expected] = table[i];

        if (!UdfTestHarness::ValidateUdf<StringVal, StringVal, StringVal, StringVal, StringVal>(
                Cut_Paste_Output, arg0_s, arg1_s, arg2_s, arg3_s, expected
            )) {
            cout << "UDX cut_paste(ssss)->s failed:\n\t|" << arg0_s.ptr << "|\n\t|" << arg1_s.ptr
                 << "|\n\t|" << arg2_s.ptr << "|\n\t|" << arg3_s.ptr << "|\n\t|" << expected.ptr
                 << "|\n";
            passing = false;
        }
    }

    return passing;
}

bool test__deletion_events() {
    int passing = true;

    std::tuple<StringVal, IntVal> table[7] = {
        std::make_tuple("ATG---AGG---GGG--TAG", 3),
        std::make_tuple("", 0),
        std::make_tuple(StringVal::null(), IntVal::null()),
        std::make_tuple("...ATG----TAG...", 1),
        std::make_tuple("ATG---...", 0),
        std::make_tuple("ATG------AGG---gac", 2),
        std::make_tuple("Stark!", 0)
    };
    for (int i = 0; i < 7; i++) {
        auto [arg0_s, expected] = table[i];

        if (!UdfTestHarness::ValidateUdf<IntVal, StringVal>(Number_Deletions, arg0_s, expected)) {
            cout << "UDX deletion_events(s)->i failed:\n\t|" << arg0_s.ptr << "|\n\t|"
                 << expected.val << "|\n";
            passing = false;
        }
    }

    return passing;
}


bool test__hamming_distance() {
    int passing = true;

    std::tuple<StringVal, StringVal, IntVal> table[8] = {
        std::make_tuple("ATGAGGCAG", "ATcAGGCrG", 2),
        std::make_tuple(StringVal::null(), "ATcAGGCrG", IntVal::null()),
        std::make_tuple("ATGAGGCAG", StringVal::null(), IntVal::null()),
        std::make_tuple("", "ATcAGGCrG", IntVal::null()),
        std::make_tuple("ATGAGGCAG", "", IntVal::null()),
        std::make_tuple("ATGAGGCAG", "ATcAGGCrGnnn", 2),
        std::make_tuple("AGCT.", "AGCTN", 0),
        std::make_tuple("AGCT-", "AGCTN", 1)
    };
    for (int i = 0; i < 8; i++) {
        auto [arg0_s, arg1_s, expected] = table[i];

        if (!UdfTestHarness::ValidateUdf<IntVal, StringVal, StringVal>(
                Hamming_Distance, arg0_s, arg1_s, expected
            )) {
            cout << "UDX hamming_distance(ss)->i failed:\n\t|" << arg0_s.ptr << "|\n\t|"
                 << arg1_s.ptr << "|\n\t|" << expected.val << "|\n";
            passing = false;
        }
    }

    return passing;
}


bool test__hamming_distance_pds() {
    int passing = true;

    std::tuple<StringVal, StringVal, StringVal, IntVal> table[11] = {
        std::make_tuple("ATGAGGCAG", "ATcAGGCrG", "-", 2),
        std::make_tuple(StringVal::null(), "ATcAGGCrG", "-", IntVal::null()),
        std::make_tuple("ATGAGGCAG", StringVal::null(), "-", IntVal::null()),
        std::make_tuple("", "ATcAGGCrG", "-", IntVal::null()),
        std::make_tuple("ATGAGGCAG", "", "-", IntVal::null()),
        std::make_tuple("ATGAGGCAG", "ATcAGGCrGnnn", "-", 2),
        std::make_tuple("AGCTN", "AGCT.", "-", 1),
        std::make_tuple("AGCTN", "AGCT-", "-", 0),
        std::make_tuple("AGCTNNN", "AGCT.-~", "-", 2),
        std::make_tuple("AGCTNNN", "AGCT.-~", "-.~", 0),
        std::make_tuple("AGC", "AGX", "", 1)
    };
    for (int i = 0; i < 11; i++) {
        auto [arg0_s, arg1_s, arg2_s, expected] = table[i];

        if (!UdfTestHarness::ValidateUdf<IntVal, StringVal, StringVal, StringVal>(
                Hamming_Distance_Pairwise_Delete, arg0_s, arg1_s, arg2_s, expected
            )) {
            cout << "UDX hamming_distance_pds(sss)->i failed:\n\t|" << arg0_s.ptr << "|\n\t|"
                 << arg1_s.ptr << "|\n\t|" << arg2_s.ptr << "|\n\t|" << expected.val << "|\n";
            passing = false;
        }
    }

    return passing;
}

bool test__is_element() {
    int passing = true;

    std::tuple<StringVal, StringVal, StringVal, BooleanVal> table[14] = {
        std::make_tuple("whales", "BABY;WHALES;FISH", ";", false),
        std::make_tuple("whales", "baby;whales;fish", ";", true),
        std::make_tuple("baby whales", "baby;whales;fish", ";", false),
        std::make_tuple(StringVal::null(), "whales;baby", ";", BooleanVal::null()),
        std::make_tuple("whales", StringVal::null(), ";", BooleanVal::null()),
        std::make_tuple("whales", "whales;baby", StringVal::null(), BooleanVal::null()),
        std::make_tuple("", "whales;baby", ";", false),
        std::make_tuple("baby whales", "", ";", false),
        std::make_tuple("y", "xz", "", false),
        std::make_tuple("y", "xyz", "", true),
        std::make_tuple("y", "xYz", "", false),
        std::make_tuple("300028908", "28907::28906::28905", "::", false),
        std::make_tuple("300028908", "28908::28907::28906::28905", "::", false),
        std::make_tuple("300028908", "300028908::300028907::300028906::300028905", "::", true)
    };

    for (int i = 0; i < 14; i++) {
        auto [mystring, list, delim, expected] = table[i];
        if (!UdfTestHarness::ValidateUdf<BooleanVal, StringVal, StringVal, StringVal>(
                Is_An_Element, mystring, list, delim, expected
            )) {
            cout << "UDX is_element(SSS)->B failed:\n\t|" << mystring.ptr << "|\n\t|" << list.ptr
                 << "|\n\t|" << delim.ptr << "|\n\t|" << expected.val << "|\n";
            passing = false;
        }
    }

    return passing;
}

bool test__longest_deletion() {
    int passing = true;

    std::tuple<StringVal, IntVal> table[7] = {
        std::make_tuple("ATG---AGG---GGG--TAG", 3),
        std::make_tuple("", 0),
        std::make_tuple(StringVal::null(), IntVal::null()),
        std::make_tuple("...ATG----TAG...", 4),
        std::make_tuple("ATG---...", 0),
        std::make_tuple("ATG------AGG---gac", 6),
        std::make_tuple("Stark!", 0)
    };
    for (int i = 0; i < 7; i++) {
        auto [arg0_s, expected] = table[i];

        if (!UdfTestHarness::ValidateUdf<IntVal, StringVal>(Longest_Deletion, arg0_s, expected)) {
            cout << "UDX longest_deletion(s)->i failed:\n\t|" << arg0_s.ptr << "|\n\t|"
                 << expected.val << "|\n";
            passing = false;
        }
    }

    return passing;
}

// test__md5()

bool test__mutation_list() {
    int passing = true;

    std::tuple<StringVal, StringVal, StringVal> table[6] = {
        std::make_tuple("ATGAGGCAG", "ATcAGGCrG", "G3C, A8R"),
        std::make_tuple(StringVal::null(), "ATcAGGCrG", StringVal::null()),
        std::make_tuple("ATGAGGCAG", StringVal::null(), StringVal::null()),
        std::make_tuple("", "ATcAGGCrG", StringVal::null()),
        std::make_tuple("ATGAGGCAG", "", StringVal::null()),
        std::make_tuple("ATGAGGCAG", "ATcAGGCrGnnn", "G3C, A8R")
    };

    for (int i = 0; i < 6; i++) {
        auto [arg0_s, arg1_s, expected] = table[i];

        if (!UdfTestHarness::ValidateUdf<StringVal, StringVal, StringVal>(
                Mutation_List_Strict, arg0_s, arg1_s, expected
            )) {
            cout << "UDX mutation_list_strict(ss)->s failed:\n\t|" << arg0_s.ptr << "|\n\t|"
                 << arg1_s.ptr << "|\n\t|" << expected.ptr << "|\n";
            passing = false;
        }
    }

    return passing;
}

bool test__mutation_list_range() {
    int passing = true;

    std::tuple<StringVal, StringVal, StringVal, StringVal> table[12] = {
        std::make_tuple("ATGAGGCAG", "ATcAGGCrG", "1..4", "G3C"),
        std::make_tuple(StringVal::null(), "ATcAGGCrG", "1..4", StringVal::null()),
        std::make_tuple("ATGAGGCAG", StringVal::null(), "1..4", StringVal::null()),
        std::make_tuple("ATGAGGCAG", "ATcAGGCrG", StringVal::null(), StringVal::null()),
        std::make_tuple("", "ATcAGGCrG", "1..4", StringVal::null()),
        std::make_tuple("ATGAGGCAG", "", "1..4", StringVal::null()),
        std::make_tuple("ATGAGGCAG", "ATcAGGCrG", "", StringVal::null()),
        std::make_tuple("ATGAGGCAG", "ATcAGGCrGnnn", "1..4", "G3C"),
        std::make_tuple("ATGAGGCAG", "ATcAGGCrG", "1..9", "G3C, A8R"),
        std::make_tuple("ATGAGGCAG", "ATcAGGCrG", "1..9,3,8", "G3C, A8R, G3C, A8R"),
        std::make_tuple("ATGAGGCAG", "ATcAGGCrG", "8,1..4", "A8R, G3C"),
        std::make_tuple("ATGAGGCAG", "ATcAGGCrG", "1..4,Stark", StringVal::null())
    };

    for (int i = 0; i < 12; i++) {
        auto [arg0_s, arg1_s, arg2_s, expected] = table[i];

        if (!UdfTestHarness::ValidateUdf<StringVal, StringVal, StringVal, StringVal>(
                Mutation_List_Strict_Range, arg0_s, arg1_s, arg2_s, expected
            )) {
            cout << "UDX mutation_list(sss)->s failed:\n\t|" << arg0_s.ptr << "|\n\t|" << arg1_s.ptr
                 << "|\n\t|" << arg2_s.ptr << "|\n\t|" << expected.ptr << "|\n";
            passing = false;
        }
    }

    return passing;
}


bool test__mutation_list_gly() {
    int passing = true;

    std::tuple<StringVal, StringVal, StringVal> table[33] = {
        std::make_tuple("NRMANHSSELL", "NRMANHSSELL", ""),
        std::make_tuple(StringVal::null(), "NRMANHSSELL", StringVal::null()),
        std::make_tuple("NRMANHSSELL", StringVal::null(), StringVal::null()),
        std::make_tuple("", "NRMANHSSELL", StringVal::null()),
        std::make_tuple("NRMANHSSELL", "", StringVal::null()),
        std::make_tuple("NRMANHSSELL", "NRMAN", ""),
        std::make_tuple("NRMANHSSELL", "NRSANPSSELL", "M3S(CHO+), H6P(CHO-)"),
        std::make_tuple(
            "NRMANHSSELL", "NXTANHSSNAT", "R2X, M3T(CHO+), E9N(CHO+), L10A, L11T(CHO+)"
        ),
        std::make_tuple("NANHSSELL", "NATHSSELL", "N3T(CHO+/-)"),
        std::make_tuple("AAA", "NIT", "A1N(CHO+), A2I, A3T(CHO+)"),
        std::make_tuple("APA", "NIT", "A1N(CHO+), P2I(CHO+), A3T(CHO+)"),
        std::make_tuple("NAA", "NIT", "A2I, A3T(CHO+)"),
        std::make_tuple("NPA", "NIT", "P2I(CHO+), A3T(CHO+)"),
        std::make_tuple("NAT", "NIS", "A2I, T3S"),
        std::make_tuple("NPT", "NIT", "P2I(CHO+)"),
        std::make_tuple("NAT", "NAP", "T3P(CHO-)"),
        std::make_tuple("NSS", "NSP", "S3P(CHO-)"),
        std::make_tuple("NPS", "NNS", "P2N(CHO+)"),
        std::make_tuple("NNS", "NPS", "N2P(CHO-)"),
        std::make_tuple("NSFT", "N-FT", "S2-(CHO+)"),
        std::make_tuple("NFST", "NF-T", "S3-(CHO+/-)"),
        std::make_tuple("NFT", "N-T", "F2-(CHO-)"),
        std::make_tuple("NFT", "-FT", "N1-(CHO-)"),
        std::make_tuple("NFT", "N-T", "F2-(CHO-)"),
        std::make_tuple("NFT", "NF-", "T3-(CHO-)"),
        std::make_tuple("NFT", "NF.", ""),
        std::make_tuple("NF.", "NFT", ""),
        std::make_tuple("NFT", ".FT", ""),
        std::make_tuple(".FT", "NFT", ""),
        std::make_tuple("NSFTNFT", "N-FTN-T", "S2-(CHO+), F6-(CHO-)"),
        std::make_tuple("AANFTDPLINSFTNFT", "AAN-TAPLINSFTNFT", "F4-(CHO-), D6A"),
        std::make_tuple("SYTNFTRGVYYPDKV-R", "SYTN-TRGVYYPDK-FR", "F5-(CHO-)"),
        std::make_tuple("SYTNSFTRGVYYPDKVFR", "SYTN-FTRGVYYPDKGFR", "S5-(CHO+), V16G")
    };

    for (int i = 0; i < 33; i++) {
        auto [arg0_s, arg1_s, expected] = table[i];

        if (!UdfTestHarness::ValidateUdf<StringVal, StringVal, StringVal>(
                Mutation_List_Strict_GLY, arg0_s, arg1_s, expected
            )) {
            cout << "UDX mutation_list_gly(ss)->s failed:\n\t|" << arg0_s.ptr << "|\n\t|"
                 << arg1_s.ptr << "|\n\t|" << expected.ptr << "|\n";
            passing = false;
        }
    }

    return passing;
}

bool test__mutation_list_indel_gly() {
    int passing = true;

    std::tuple<StringVal, StringVal, StringVal> table[38] = {
        std::make_tuple("NRMANHSSELL", "NRMANHSSELL", ""),
        std::make_tuple(StringVal::null(), "NRMANHSSELL", StringVal::null()),
        std::make_tuple("NRMANHSSELL", StringVal::null(), StringVal::null()),
        std::make_tuple("", "NRMANHSSELL", StringVal::null()),
        std::make_tuple("NRMANHSSELL", "", StringVal::null()),
        std::make_tuple("NRMANHSSELL", "NRMAN", ""),
        std::make_tuple("NRMANHSSELL", "NRSANPSSELL", "M3S(CHO+), H6P(CHO-)"),
        std::make_tuple(
            "NRMANHSSELL", "NXTANHSSNAT", "R2X, M3T(CHO+), E9N(CHO+), L10A, L11T(CHO+)"
        ),
        std::make_tuple("NANHSSELL", "NATHSSELL", "N3T(CHO+/-)"),
        std::make_tuple("AAA", "NIT", "A1N(CHO+), A2I, A3T(CHO+)"),
        std::make_tuple("APA", "NIT", "A1N(CHO+), P2I(CHO+), A3T(CHO+)"),
        std::make_tuple("NAA", "NIT", "A2I, A3T(CHO+)"),
        std::make_tuple("NPA", "NIT", "P2I(CHO+), A3T(CHO+)"),
        std::make_tuple("NAT", "NIS", "A2I, T3S"),
        std::make_tuple("NPT", "NIT", "P2I(CHO+)"),
        std::make_tuple("NAT", "NAP", "T3P(CHO-)"),
        std::make_tuple("NSS", "NSP", "S3P(CHO-)"),
        std::make_tuple("NPS", "NNS", "P2N(CHO+)"),
        std::make_tuple("NNS", "NPS", "N2P(CHO-)"),
        std::make_tuple("NSFT", "N-FT", "S2-(CHO+)"),
        std::make_tuple("NFST", "NF-T", "S3-(CHO+/-)"),
        std::make_tuple("NFT", "N-T", "F2-(CHO-)"),
        std::make_tuple("NFT", "-FT", "N1-(CHO-)"),
        std::make_tuple("NFT", "N-T", "F2-(CHO-)"),
        std::make_tuple("NFT", "NF-", "T3-(CHO-)"),
        std::make_tuple("NFT", "NF.", ""),
        std::make_tuple("NF.", "NFT", ""),
        std::make_tuple("NFT", ".FT", ""),
        std::make_tuple(".FT", "NFT", ""),
        std::make_tuple("NSFTNFT", "N-FTN-T", "S2-(CHO+), F6-(CHO-)"),
        std::make_tuple("AANFTDPLINSFTNFT", "AAN-TAPLINSFTNFT", "F4-(CHO-), D6A"),
        std::make_tuple("SYTNFTRGVYYPDKV-R", "SYTN-TRGVYYPDK-FR", "F5-(CHO-)"),
        std::make_tuple("SYTNSFTRGVYYPDKVFR", "SYTN-FTRGVYYPDKGFR", "S5-(CHO+), V16G"),
        std::make_tuple(
            "NFSNNSTSFNNSSTNPTTT", "NFTNFSTSFNFSATNNTTT",
            "S3T, N5F(CHO-), N11F(CHO-), S13A(CHO-), P16N(CHO+)"
        ),
        std::make_tuple(
            "NNPSATNASATNASAT", "NNTNPTNANPTNANET",
            "P3T(CHO+), S4N, A5P, S9N(CHO-), A10P, S14N(CHO+/-), A15E"
        ),
        std::make_tuple(
            "N-SSTN-SSTN--AT", "NA-STNP-STNPTAT", "-7P(CHO-), S8-(CHO-), -12P(CHO-), -13T(CHO-)"
        ),
        std::make_tuple("NXTTNPST", "NPSTNXTS", "X2P(CHO-), T3S, P6X(CHO+), S7T, T8S"),
        std::make_tuple("N--ASN--ASS", "NPNASNP-NAS", "-7P(CHO-), A9N(CHO+), S10A(CHO-)")
    };

    for (int i = 0; i < 38; i++) {
        auto [arg0_s, arg1_s, expected] = table[i];

        if (!UdfTestHarness::ValidateUdf<StringVal, StringVal, StringVal>(
                Mutation_List_Indel_GLY, arg0_s, arg1_s, expected
            )) {
            std::cout << "UDX mutation_list_indel_gly(ss)->s failed:\n\t|" << arg0_s.ptr << "|\n\t|"
                      << arg1_s.ptr << "|\n\t|" << expected.ptr << "|\n";
            passing = false;
        }
    }

    return passing;
}

bool test__mutation_list_nt() {
    int passing = true;

    std::tuple<StringVal, StringVal, StringVal> table[7] = {
        std::make_tuple("ATGAGGCAG", "ATcAGGCrG", "G3C"),
        std::make_tuple(StringVal::null(), "ATcAGGCrG", StringVal::null()),
        std::make_tuple("ATGAGGCAG", StringVal::null(), StringVal::null()),
        std::make_tuple("", "ATcAGGCrG", StringVal::null()),
        std::make_tuple("ATGAGGCAG", "", StringVal::null()),
        std::make_tuple("ATGAGGCAG", "ATcAGGCrGnnn", "G3C"),
        std::make_tuple(
            "NNNNNNNNNNNNNNNBBBBBBBDDDDDDDHHHHHHHVVVVVVRRYYYSSWWWKKKMM",
            "acgturyswkmbdhvcgtuyskagturwkactuywmacgrsmagctucgatugtuac", ""
        )
    };

    for (int i = 0; i < 7; i++) {
        auto [arg0_s, arg1_s, expected] = table[i];

        if (!UdfTestHarness::ValidateUdf<StringVal, StringVal, StringVal>(
                Mutation_List_No_Ambiguous, arg0_s, arg1_s, expected
            )) {
            cout << "UDX mutation_list_nt(ss)->s failed:\n\t|" << arg0_s.ptr << "|\n\t|"
                 << arg1_s.ptr << "|\n\t|" << expected.ptr << "|\n";
            passing = false;
        }
    }

    return passing;
}

bool test__nt_distance() {
    int passing = true;

    std::tuple<StringVal, StringVal, IntVal> table[9] = {
        std::make_tuple("ATGAGGCAG", "ATcAGGCrG", 1),
        std::make_tuple(StringVal::null(), "ATcAGGCrG", IntVal::null()),
        std::make_tuple("ATGAGGCAG", StringVal::null(), IntVal::null()),
        std::make_tuple("", "ATcAGGCrG", IntVal::null()),
        std::make_tuple("ATGAGGCAG", "", IntVal::null()),
        std::make_tuple("ATGAGGCAG", "ATcAGGCrGnnn", 1),
        std::make_tuple("AGCT.", "AGCTN", 0),
        std::make_tuple("AGCT-", "AGCTN", 1),
        std::make_tuple(
            "NNNNNNNNNNNNNNNBBBBBBBDDDDDDDHHHHHHHVVVVVVRRYYYSSWWWKKKMM",
            "acgturyswkmbdhvcgtuyskagturwkactuywmacgrsmagctucgatugtuac", 0
        )
    };
    for (int i = 0; i < 9; i++) {
        auto [arg0_s, arg1_s, expected] = table[i];

        if (!UdfTestHarness::ValidateUdf<IntVal, StringVal, StringVal>(
                Nt_Distance, arg0_s, arg1_s, expected
            )) {
            cout << "UDX nt_distance(ss)->i failed:\n\t|" << arg0_s.ptr << "|\n\t|" << arg1_s.ptr
                 << "|\n\t|" << expected.val << "|\n";
            passing = false;
        }
    }

    return passing;
}

bool test__nt_id() {
    int passing = true;

    std::tuple<StringVal, StringVal> table[8] = {
        std::make_tuple("", StringVal::null()),
        std::make_tuple(StringVal::null(), StringVal::null()),
        std::make_tuple(
            "ATGAACACTCAAATCCTGGTATTCGCTCTGGTGGCGAGCATTCCGACAAATGCA",
            "198a9b787a7e856b54eea10948bfa6bda5882681"
        ),
        std::make_tuple(
            "ATGAACACTCAAATCCTGGTATTCGCTCTGGTGGCGAGCATTCCGACAAATGCA...   ---~~~:::",
            "198a9b787a7e856b54eea10948bfa6bda5882681"
        ),
        std::make_tuple(
            "atgaacactcaaatcctggtattcgctctggtggcgagcattccgacaaatgca",
            "198a9b787a7e856b54eea10948bfa6bda5882681"
        ),
        std::make_tuple(
            "ATGAACACTCAAATCCTGGTATTCGCTCTGGTGGCGAGCATTCCGACAAATGCg",
            "6f8a94f328a6374ab9af217e0dd2ea85b00f176a"
        ),
        std::make_tuple("1", "356a192b7913b04c54574d18c28d46e6395428ab"),
        std::make_tuple("TCC ACC GCC CGG AAA", "a3505a17b5b0adf08a7d43667e0802a05beedc8c")
    };
    for (int i = 0; i < 8; i++) {
        auto [arg0_s, expected] = table[i];

        if (!UdfTestHarness::ValidateUdf<StringVal, StringVal>(nt_id, arg0_s, expected)) {
            cout << "UDX nt_id(s)->s failed:\n\t|" << arg0_s.ptr << "|\n\t|" << expected.ptr
                 << "|\n";
            passing = false;
        }
    }

    return passing;
}

bool test__pcd() {
    int passing = true;

    std::tuple<StringVal, StringVal, DoubleVal> table[473] = {
        std::make_tuple("NRMANHSSELL", "NRMANHSSELL", 0.0),
        std::make_tuple(StringVal::null(), "NRMANHSSELL", DoubleVal::null()),
        std::make_tuple("NRMANHSSELL", StringVal::null(), DoubleVal::null()),
        std::make_tuple("", "NRMANHSSELL", DoubleVal::null()),
        std::make_tuple("NRMANHSSELL", "", DoubleVal::null()),
        std::make_tuple("NRMANHSSELL", "NRMAN", 0.0),
        std::make_tuple("NRMANHSSELL", "NRSANPSSELL", 1.0972237272727272),
        std::make_tuple("NRMANHSSELL", "NXTANHSSNAT", 1.1573239000000002),
        std::make_tuple("NANHSSELL", "NATHSSELL", 0.20334544444444444),
        std::make_tuple("AAA", "NIT", 3.6861266666666666),
        std::make_tuple("APA", "NIT", 4.319184333333333),
        std::make_tuple("NAA", "NIT", 2.3855540000000004),
        std::make_tuple("NPA", "NIT", 3.0186116666666667),
        std::make_tuple("NAT", "NIS", 3.8281236666666665),
        std::make_tuple("NPT", "NIT", 1.7633426666666667),
        std::make_tuple("NAT", "NAP", 1.6795039999999999),
        std::make_tuple("NSS", "NSP", 1.1588213333333333),
        std::make_tuple("NPS", "NNS", 1.3521793333333332),
        std::make_tuple("NNS", "NPS", 1.3521793333333332),
        std::make_tuple("-", "-", 0.0),
        std::make_tuple("-", "A", 2.249089),
        std::make_tuple("-", "C", 1.965731),
        std::make_tuple("-", "D", 5.015307),
        std::make_tuple("-", "E", 2.619198),
        std::make_tuple("-", "F", 2.2953),
        std::make_tuple("-", "G", 3.159415),
        std::make_tuple("-", "H", 2.290895),
        std::make_tuple("-", "I", 2.683561),
        std::make_tuple("-", "K", 2.596979),
        std::make_tuple("-", "L", 2.596459),
        std::make_tuple("-", "M", 3.187256),
        std::make_tuple("-", "N", 2.043331),
        std::make_tuple("-", "P", 3.021241),
        std::make_tuple("-", "Q", 3.691869),
        std::make_tuple("-", "R", 3.637142),
        std::make_tuple("-", "S", 5.669383),
        std::make_tuple("-", "T", 2.74556),
        std::make_tuple("-", "V", 2.300174),
        std::make_tuple("-", "W", 2.319116),
        std::make_tuple("-", "Y", 3.654066),
        std::make_tuple("A", "-", 2.249089),
        std::make_tuple("A", "A", 0.0),
        std::make_tuple("A", "C", 3.229938),
        std::make_tuple("A", "D", 5.170251),
        std::make_tuple("A", "E", 3.364045),
        std::make_tuple("A", "F", 3.426281),
        std::make_tuple("A", "G", 4.25966),
        std::make_tuple("A", "H", 3.430656),
        std::make_tuple("A", "I", 3.390855),
        std::make_tuple("A", "K", 3.828067),
        std::make_tuple("A", "L", 1.247798),
        std::make_tuple("A", "M", 4.154732),
        std::make_tuple("A", "N", 3.901718),
        std::make_tuple("A", "P", 3.962688),
        std::make_tuple("A", "Q", 3.992255),
        std::make_tuple("A", "R", 4.650462),
        std::make_tuple("A", "S", 5.542608),
        std::make_tuple("A", "T", 3.765807),
        std::make_tuple("A", "V", 1.726268),
        std::make_tuple("A", "W", 4.167385),
        std::make_tuple("A", "Y", 5.33779),
        std::make_tuple("C", "-", 1.965731),
        std::make_tuple("C", "A", 3.229938),
        std::make_tuple("C", "C", 0.0),
        std::make_tuple("C", "D", 4.799896),
        std::make_tuple("C", "E", 4.250329),
        std::make_tuple("C", "F", 3.10295),
        std::make_tuple("C", "G", 4.096022),
        std::make_tuple("C", "H", 2.122616),
        std::make_tuple("C", "I", 3.625603),
        std::make_tuple("C", "K", 4.151819),
        std::make_tuple("C", "L", 2.884978),
        std::make_tuple("C", "M", 4.008728),
        std::make_tuple("C", "N", 3.489685),
        std::make_tuple("C", "P", 2.979329),
        std::make_tuple("C", "Q", 3.605052),
        std::make_tuple("C", "R", 5.124656),
        std::make_tuple("C", "S", 5.086767),
        std::make_tuple("C", "T", 4.165381),
        std::make_tuple("C", "V", 2.602403),
        std::make_tuple("C", "W", 2.082931),
        std::make_tuple("C", "Y", 4.640743),
        std::make_tuple("D", "-", 5.015307),
        std::make_tuple("D", "A", 5.170251),
        std::make_tuple("D", "C", 4.799896),
        std::make_tuple("D", "D", 0.0),
        std::make_tuple("D", "E", 5.956098),
        std::make_tuple("D", "F", 7.012867),
        std::make_tuple("D", "G", 7.653339),
        std::make_tuple("D", "H", 4.053677),
        std::make_tuple("D", "I", 7.50978),
        std::make_tuple("D", "K", 6.543439),
        std::make_tuple("D", "L", 4.282674),
        std::make_tuple("D", "M", 7.821502),
        std::make_tuple("D", "N", 6.503038),
        std::make_tuple("D", "P", 3.451637),
        std::make_tuple("D", "Q", 1.630031),
        std::make_tuple("D", "R", 8.073717),
        std::make_tuple("D", "S", 2.295953),
        std::make_tuple("D", "T", 7.595762),
        std::make_tuple("D", "V", 4.685435),
        std::make_tuple("D", "W", 5.866515),
        std::make_tuple("D", "Y", 8.336756),
        std::make_tuple("E", "-", 2.619198),
        std::make_tuple("E", "A", 3.364045),
        std::make_tuple("E", "C", 4.250329),
        std::make_tuple("E", "D", 5.956098),
        std::make_tuple("E", "E", 0.0),
        std::make_tuple("E", "F", 2.889152),
        std::make_tuple("E", "G", 4.685478),
        std::make_tuple("E", "H", 3.885589),
        std::make_tuple("E", "I", 3.290365),
        std::make_tuple("E", "K", 2.875361),
        std::make_tuple("E", "L", 4.020771),
        std::make_tuple("E", "M", 3.176445),
        std::make_tuple("E", "N", 2.934314),
        std::make_tuple("E", "P", 4.888814),
        std::make_tuple("E", "Q", 4.832194),
        std::make_tuple("E", "R", 4.007668),
        std::make_tuple("E", "S", 7.292318),
        std::make_tuple("E", "T", 3.300894),
        std::make_tuple("E", "V", 3.767307),
        std::make_tuple("E", "W", 3.475989),
        std::make_tuple("E", "Y", 3.931641),
        std::make_tuple("F", "-", 2.2953),
        std::make_tuple("F", "A", 3.426281),
        std::make_tuple("F", "C", 3.10295),
        std::make_tuple("F", "D", 7.012867),
        std::make_tuple("F", "E", 2.889152),
        std::make_tuple("F", "F", 0.0),
        std::make_tuple("F", "G", 3.248554),
        std::make_tuple("F", "H", 3.988734),
        std::make_tuple("F", "I", 0.950947),
        std::make_tuple("F", "K", 3.386458),
        std::make_tuple("F", "L", 4.031303),
        std::make_tuple("F", "M", 1.452033),
        std::make_tuple("F", "N", 2.555269),
        std::make_tuple("F", "P", 4.987153),
        std::make_tuple("F", "Q", 5.749722),
        std::make_tuple("F", "R", 3.720376),
        std::make_tuple("F", "S", 7.700617),
        std::make_tuple("F", "T", 2.106015),
        std::make_tuple("F", "V", 3.404174),
        std::make_tuple("F", "W", 2.314627),
        std::make_tuple("F", "Y", 2.548921),
        std::make_tuple("G", "-", 3.159415),
        std::make_tuple("G", "A", 4.25966),
        std::make_tuple("G", "C", 4.096022),
        std::make_tuple("G", "D", 7.653339),
        std::make_tuple("G", "E", 4.685478),
        std::make_tuple("G", "F", 3.248554),
        std::make_tuple("G", "G", 0.0),
        std::make_tuple("G", "H", 4.973258),
        std::make_tuple("G", "I", 2.862307),
        std::make_tuple("G", "K", 3.513574),
        std::make_tuple("G", "L", 4.930933),
        std::make_tuple("G", "M", 3.985662),
        std::make_tuple("G", "N", 2.281995),
        std::make_tuple("G", "P", 4.644438),
        std::make_tuple("G", "Q", 6.449124),
        std::make_tuple("G", "R", 2.777967),
        std::make_tuple("G", "S", 7.713728),
        std::make_tuple("G", "T", 1.794826),
        std::make_tuple("G", "V", 4.381997),
        std::make_tuple("G", "W", 4.278271),
        std::make_tuple("G", "Y", 2.844205),
        std::make_tuple("H", "-", 2.290895),
        std::make_tuple("H", "A", 3.430656),
        std::make_tuple("H", "C", 2.122616),
        std::make_tuple("H", "D", 4.053677),
        std::make_tuple("H", "E", 3.885589),
        std::make_tuple("H", "F", 3.988734),
        std::make_tuple("H", "G", 4.973258),
        std::make_tuple("H", "H", 0.0),
        std::make_tuple("H", "I", 4.606832),
        std::make_tuple("H", "K", 3.389498),
        std::make_tuple("H", "L", 3.224376),
        std::make_tuple("H", "M", 4.383811),
        std::make_tuple("H", "N", 3.66955),
        std::make_tuple("H", "P", 3.400338),
        std::make_tuple("H", "Q", 2.505015),
        std::make_tuple("H", "R", 4.91396),
        std::make_tuple("H", "S", 4.936588),
        std::make_tuple("H", "T", 4.832215),
        std::make_tuple("H", "V", 3.585443),
        std::make_tuple("H", "W", 2.643804),
        std::make_tuple("H", "Y", 5.219847),
        std::make_tuple("I", "-", 2.683561),
        std::make_tuple("I", "A", 3.390855),
        std::make_tuple("I", "C", 3.625603),
        std::make_tuple("I", "D", 7.50978),
        std::make_tuple("I", "E", 3.290365),
        std::make_tuple("I", "F", 0.950947),
        std::make_tuple("I", "G", 2.862307),
        std::make_tuple("I", "H", 4.606832),
        std::make_tuple("I", "I", 0.0),
        std::make_tuple("I", "K", 3.622541),
        std::make_tuple("I", "L", 4.154383),
        std::make_tuple("I", "M", 1.843231),
        std::make_tuple("I", "N", 2.777607),
        std::make_tuple("I", "P", 5.290028),
        std::make_tuple("I", "Q", 6.259904),
        std::make_tuple("I", "R", 3.562906),
        std::make_tuple("I", "S", 8.025833),
        std::make_tuple("I", "T", 1.65994),
        std::make_tuple("I", "V", 3.501528),
        std::make_tuple("I", "W", 3.194558),
        std::make_tuple("I", "Y", 2.661635),
        std::make_tuple("K", "-", 2.596979),
        std::make_tuple("K", "A", 3.828067),
        std::make_tuple("K", "C", 4.151819),
        std::make_tuple("K", "D", 6.543439),
        std::make_tuple("K", "E", 2.875361),
        std::make_tuple("K", "F", 3.386458),
        std::make_tuple("K", "G", 3.513574),
        std::make_tuple("K", "H", 3.389498),
        std::make_tuple("K", "I", 3.622541),
        std::make_tuple("K", "K", 0.0),
        std::make_tuple("K", "L", 4.628726),
        std::make_tuple("K", "M", 3.271743),
        std::make_tuple("K", "N", 1.957013),
        std::make_tuple("K", "P", 4.904732),
        std::make_tuple("K", "Q", 5.077834),
        std::make_tuple("K", "R", 1.831912),
        std::make_tuple("K", "S", 7.447268),
        std::make_tuple("K", "T", 2.933564),
        std::make_tuple("K", "V", 4.695817),
        std::make_tuple("K", "W", 3.608435),
        std::make_tuple("K", "Y", 3.366764),
        std::make_tuple("L", "-", 2.596459),
        std::make_tuple("L", "A", 1.247798),
        std::make_tuple("L", "C", 2.884978),
        std::make_tuple("L", "D", 4.282674),
        std::make_tuple("L", "E", 4.020771),
        std::make_tuple("L", "F", 4.031303),
        std::make_tuple("L", "G", 4.930933),
        std::make_tuple("L", "H", 3.224376),
        std::make_tuple("L", "I", 4.154383),
        std::make_tuple("L", "K", 4.628726),
        std::make_tuple("L", "L", 0.0),
        std::make_tuple("L", "M", 4.900633),
        std::make_tuple("L", "N", 4.533056),
        std::make_tuple("L", "P", 3.443298),
        std::make_tuple("L", "Q", 3.27492),
        std::make_tuple("L", "R", 5.628819),
        std::make_tuple("L", "S", 4.504032),
        std::make_tuple("L", "T", 4.649613),
        std::make_tuple("L", "V", 1.292594),
        std::make_tuple("L", "W", 4.245197),
        std::make_tuple("L", "Y", 6.042466),
        std::make_tuple("M", "-", 3.187256),
        std::make_tuple("M", "A", 4.154732),
        std::make_tuple("M", "C", 4.008728),
        std::make_tuple("M", "D", 7.821502),
        std::make_tuple("M", "E", 3.176445),
        std::make_tuple("M", "F", 1.452033),
        std::make_tuple("M", "G", 3.985662),
        std::make_tuple("M", "H", 4.383811),
        std::make_tuple("M", "I", 1.843231),
        std::make_tuple("M", "K", 3.271743),
        std::make_tuple("M", "L", 4.900633),
        std::make_tuple("M", "M", 0.0),
        std::make_tuple("M", "N", 3.121698),
        std::make_tuple("M", "P", 6.108183),
        std::make_tuple("M", "Q", 6.426531),
        std::make_tuple("M", "R", 3.528257),
        std::make_tuple("M", "S", 8.669123),
        std::make_tuple("M", "T", 2.741514),
        std::make_tuple("M", "V", 4.558618),
        std::make_tuple("M", "W", 2.816647),
        std::make_tuple("M", "Y", 2.694847),
        std::make_tuple("N", "-", 2.043331),
        std::make_tuple("N", "A", 3.901718),
        std::make_tuple("N", "C", 3.489685),
        std::make_tuple("N", "D", 6.503038),
        std::make_tuple("N", "E", 2.934314),
        std::make_tuple("N", "F", 2.555269),
        std::make_tuple("N", "G", 2.281995),
        std::make_tuple("N", "H", 3.66955),
        std::make_tuple("N", "I", 2.777607),
        std::make_tuple("N", "K", 1.957013),
        std::make_tuple("N", "L", 4.533056),
        std::make_tuple("N", "M", 3.121698),
        std::make_tuple("N", "N", 0.0),
        std::make_tuple("N", "P", 4.056538),
        std::make_tuple("N", "Q", 5.237738),
        std::make_tuple("N", "R", 2.330923),
        std::make_tuple("N", "S", 7.20853),
        std::make_tuple("N", "T", 1.830109),
        std::make_tuple("N", "V", 4.079951),
        std::make_tuple("N", "W", 2.923269),
        std::make_tuple("N", "Y", 2.12165),
        std::make_tuple("P", "-", 3.021241),
        std::make_tuple("P", "A", 3.962688),
        std::make_tuple("P", "C", 2.979329),
        std::make_tuple("P", "D", 3.451637),
        std::make_tuple("P", "E", 4.888814),
        std::make_tuple("P", "F", 4.987153),
        std::make_tuple("P", "G", 4.644438),
        std::make_tuple("P", "H", 3.400338),
        std::make_tuple("P", "I", 5.290028),
        std::make_tuple("P", "K", 4.904732),
        std::make_tuple("P", "L", 3.443298),
        std::make_tuple("P", "M", 6.108183),
        std::make_tuple("P", "N", 4.056538),
        std::make_tuple("P", "P", 0.0),
        std::make_tuple("P", "Q", 2.935575),
        std::make_tuple("P", "R", 5.882474),
        std::make_tuple("P", "S", 3.476464),
        std::make_tuple("P", "T", 5.038512),
        std::make_tuple("P", "V", 3.128562),
        std::make_tuple("P", "W", 4.262112),
        std::make_tuple("P", "Y", 5.825624),
        std::make_tuple("Q", "-", 3.691869),
        std::make_tuple("Q", "A", 3.992255),
        std::make_tuple("Q", "C", 3.605052),
        std::make_tuple("Q", "D", 1.630031),
        std::make_tuple("Q", "E", 4.832194),
        std::make_tuple("Q", "F", 5.749722),
        std::make_tuple("Q", "G", 6.449124),
        std::make_tuple("Q", "H", 2.505015),
        std::make_tuple("Q", "I", 6.259904),
        std::make_tuple("Q", "K", 5.077834),
        std::make_tuple("Q", "L", 3.27492),
        std::make_tuple("Q", "M", 6.426531),
        std::make_tuple("Q", "N", 5.237738),
        std::make_tuple("Q", "P", 2.935575),
        std::make_tuple("Q", "Q", 0.0),
        std::make_tuple("Q", "R", 6.646255),
        std::make_tuple("Q", "S", 2.985532),
        std::make_tuple("Q", "T", 6.356398),
        std::make_tuple("Q", "V", 3.82616),
        std::make_tuple("Q", "W", 4.622251),
        std::make_tuple("Q", "Y", 7.08564),
        std::make_tuple("R", "-", 3.637142),
        std::make_tuple("R", "A", 4.650462),
        std::make_tuple("R", "C", 5.124656),
        std::make_tuple("R", "D", 8.073717),
        std::make_tuple("R", "E", 4.007668),
        std::make_tuple("R", "F", 3.720376),
        std::make_tuple("R", "G", 2.777967),
        std::make_tuple("R", "H", 4.91396),
        std::make_tuple("R", "I", 3.562906),
        std::make_tuple("R", "K", 1.831912),
        std::make_tuple("R", "L", 5.628819),
        std::make_tuple("R", "M", 3.528257),
        std::make_tuple("R", "N", 2.330923),
        std::make_tuple("R", "P", 5.882474),
        std::make_tuple("R", "Q", 6.646255),
        std::make_tuple("R", "R", 0.0),
        std::make_tuple("R", "S", 8.677989),
        std::make_tuple("R", "T", 2.422829),
        std::make_tuple("R", "V", 5.518152),
        std::make_tuple("R", "W", 4.622196),
        std::make_tuple("R", "Y", 2.925919),
        std::make_tuple("S", "-", 5.669383),
        std::make_tuple("S", "A", 5.542608),
        std::make_tuple("S", "C", 5.086767),
        std::make_tuple("S", "D", 2.295953),
        std::make_tuple("S", "E", 7.292318),
        std::make_tuple("S", "F", 7.700617),
        std::make_tuple("S", "G", 7.713728),
        std::make_tuple("S", "H", 4.936588),
        std::make_tuple("S", "I", 8.025833),
        std::make_tuple("S", "K", 7.447268),
        std::make_tuple("S", "L", 4.504032),
        std::make_tuple("S", "M", 8.669123),
        std::make_tuple("S", "N", 7.20853),
        std::make_tuple("S", "P", 3.476464),
        std::make_tuple("S", "Q", 2.985532),
        std::make_tuple("S", "R", 8.677989),
        std::make_tuple("S", "S", 0.0),
        std::make_tuple("S", "T", 8.093516),
        std::make_tuple("S", "V", 4.911201),
        std::make_tuple("S", "W", 6.744983),
        std::make_tuple("S", "Y", 9.051536),
        std::make_tuple("T", "-", 2.74556),
        std::make_tuple("T", "A", 3.765807),
        std::make_tuple("T", "C", 4.165381),
        std::make_tuple("T", "D", 7.595762),
        std::make_tuple("T", "E", 3.300894),
        std::make_tuple("T", "F", 2.106015),
        std::make_tuple("T", "G", 1.794826),
        std::make_tuple("T", "H", 4.832215),
        std::make_tuple("T", "I", 1.65994),
        std::make_tuple("T", "K", 2.933564),
        std::make_tuple("T", "L", 4.649613),
        std::make_tuple("T", "M", 2.741514),
        std::make_tuple("T", "N", 1.830109),
        std::make_tuple("T", "P", 5.038512),
        std::make_tuple("T", "Q", 6.356398),
        std::make_tuple("T", "R", 2.422829),
        std::make_tuple("T", "S", 8.093516),
        std::make_tuple("T", "T", 0.0),
        std::make_tuple("T", "V", 4.045306),
        std::make_tuple("T", "W", 3.776321),
        std::make_tuple("T", "Y", 2.05638),
        std::make_tuple("V", "-", 2.300174),
        std::make_tuple("V", "A", 1.726268),
        std::make_tuple("V", "C", 2.602403),
        std::make_tuple("V", "D", 4.685435),
        std::make_tuple("V", "E", 3.767307),
        std::make_tuple("V", "F", 3.404174),
        std::make_tuple("V", "G", 4.381997),
        std::make_tuple("V", "H", 3.585443),
        std::make_tuple("V", "I", 3.501528),
        std::make_tuple("V", "K", 4.695817),
        std::make_tuple("V", "L", 1.292594),
        std::make_tuple("V", "M", 4.558618),
        std::make_tuple("V", "N", 4.079951),
        std::make_tuple("V", "P", 3.128562),
        std::make_tuple("V", "Q", 3.82616),
        std::make_tuple("V", "R", 5.518152),
        std::make_tuple("V", "S", 4.911201),
        std::make_tuple("V", "T", 4.045306),
        std::make_tuple("V", "V", 0.0),
        std::make_tuple("V", "W", 3.823493),
        std::make_tuple("V", "Y", 5.38897),
        std::make_tuple("W", "-", 2.319116),
        std::make_tuple("W", "A", 4.167385),
        std::make_tuple("W", "C", 2.082931),
        std::make_tuple("W", "D", 5.866515),
        std::make_tuple("W", "E", 3.475989),
        std::make_tuple("W", "F", 2.314627),
        std::make_tuple("W", "G", 4.278271),
        std::make_tuple("W", "H", 2.643804),
        std::make_tuple("W", "I", 3.194558),
        std::make_tuple("W", "K", 3.608435),
        std::make_tuple("W", "L", 4.245197),
        std::make_tuple("W", "M", 2.816647),
        std::make_tuple("W", "N", 2.923269),
        std::make_tuple("W", "P", 4.262112),
        std::make_tuple("W", "Q", 4.622251),
        std::make_tuple("W", "R", 4.622196),
        std::make_tuple("W", "S", 6.744983),
        std::make_tuple("W", "T", 3.776321),
        std::make_tuple("W", "V", 3.823493),
        std::make_tuple("W", "W", 0.0),
        std::make_tuple("W", "Y", 3.440509),
        std::make_tuple("Y", "-", 3.654066),
        std::make_tuple("Y", "A", 5.33779),
        std::make_tuple("Y", "C", 4.640743),
        std::make_tuple("Y", "D", 8.336756),
        std::make_tuple("Y", "E", 3.931641),
        std::make_tuple("Y", "F", 2.548921),
        std::make_tuple("Y", "G", 2.844205),
        std::make_tuple("Y", "H", 5.219847),
        std::make_tuple("Y", "I", 2.661635),
        std::make_tuple("Y", "K", 3.366764),
        std::make_tuple("Y", "L", 6.042466),
        std::make_tuple("Y", "M", 2.694847),
        std::make_tuple("Y", "N", 2.12165),
        std::make_tuple("Y", "P", 5.825624),
        std::make_tuple("Y", "Q", 7.08564),
        std::make_tuple("Y", "R", 2.925919),
        std::make_tuple("Y", "S", 9.051536),
        std::make_tuple("Y", "T", 2.05638),
        std::make_tuple("Y", "V", 5.38897),
        std::make_tuple("Y", "W", 3.440509),
        std::make_tuple("Y", "Y", 0.0),
        std::make_tuple("X", "X", DoubleVal::null()),
        std::make_tuple("X", "P", DoubleVal::null()),
        std::make_tuple("X", "Q", DoubleVal::null()),
        std::make_tuple("X", "R", DoubleVal::null()),
        std::make_tuple("X", "S", DoubleVal::null()),
        std::make_tuple("X", "T", DoubleVal::null()),
        std::make_tuple("X", "V", DoubleVal::null()),
        std::make_tuple("X", "W", DoubleVal::null()),
        std::make_tuple("X", "Y", DoubleVal::null()),
        std::make_tuple("", "X", DoubleVal::null()),
        std::make_tuple("X", "", DoubleVal::null()),
        std::make_tuple("X", StringVal::null(), DoubleVal::null()),
        std::make_tuple(StringVal::null(), "X", DoubleVal::null())
    };
    for (int i = 0; i < 473; i++) {
        auto [arg0_s, arg1_s, expected] = table[i];

        if (!UdfTestHarness::ValidateUdf<DoubleVal, StringVal, StringVal>(
                Physiochemical_Distance, arg0_s, arg1_s, expected
            )) {
            cout << "UDX pcd(ss)->d failed:\n\t|" << arg0_s.ptr << "|\n\t|" << arg1_s.ptr
                 << "|\n\t|" << expected.val << "|\n";
            passing = false;
        }
    }

    return passing;
}

bool test__range_from_list() {
    int passing = true;

    std::tuple<StringVal, StringVal, StringVal> table[9] = {
        std::make_tuple("1;2;3;5;9;10;11;12", ";", "1..3;5;9..12"),
        std::make_tuple("1;2;3;5;9;10;11;12", StringVal::null(), StringVal::null()),
        std::make_tuple(StringVal::null(), ";", StringVal::null()),
        std::make_tuple("1;2;3;5;9;10;11;12", "", "1;2;3;5;9;10;11;12"),
        std::make_tuple("", ",", ""),
        std::make_tuple("1,1,1,1,2,3,4,5,6", ",", "1..6"),
        std::make_tuple("1,2,Stark!", ",", "1..2"),
        std::make_tuple("1,2;3;4", ",;", "1"),
        std::make_tuple("1stark2stark3stark5", "stark", "1..3stark5")
    };
    for (int i = 0; i < 9; i++) {
        auto [arg0_s, arg1_s, expected] = table[i];

        if (!UdfTestHarness::ValidateUdf<StringVal, StringVal, StringVal>(
                Range_From_List, arg0_s, arg1_s, expected
            )) {
            cout << "UDX range_from_list(ss)->s failed:\n\t|" << arg0_s.ptr << "|\n\t|"
                 << arg1_s.ptr << "|\n\t|" << expected.ptr << "|\n";
            passing = false;
        }
    }

    return passing;
}

bool test__reverse_complement() {
    int passing = true;

    std::tuple<StringVal, StringVal> table[5] = {
        std::make_tuple("ATGAGG---GGGTGGTAG", "CTACCACCC---CCTCAT"),
        std::make_tuple("ctaccaccc---cctcat", "atgagg---gggtggtag"), std::make_tuple("", ""),
        std::make_tuple(StringVal::null(), StringVal::null()),
        std::make_tuple("gcatrykmbvdhuGCATRYKMBVDHU", "ADHBVKMRYATGCadhbvkmryatgc")
    };
    for (int i = 0; i < 5; i++) {
        auto [arg0_s, expected] = table[i];

        if (!UdfTestHarness::ValidateUdf<StringVal, StringVal>(Rev_Complement, arg0_s, expected)) {
            cout << "UDX reverse_complement(s)->s failed:\n\t|" << arg0_s.ptr << "|\n\t|"
                 << expected.ptr << "|\n";
            passing = false;
        }
    }

    return passing;
}

bool test__sort_alleles() {
    int passing = true;

    std::tuple<StringVal, StringVal, StringVal> table[9] = {
        std::make_tuple("89G, 56Y, 2S, 160T", ", ", "2S, 56Y, 89G, 160T"),
        std::make_tuple(StringVal::null(), ", ", StringVal::null()),
        std::make_tuple("89G, 56Y, 2S, 160T", StringVal::null(), StringVal::null()),
        std::make_tuple("A160T;S53G;M140R", "", "A160T;S53G;M140R"),
        std::make_tuple("", ";", ""),
        std::make_tuple("A160T;S53G;M140R", ";", "S53G;M140R;A160T"),
        std::make_tuple("A1starkC3starkA2", "stark", "A1starkA2starkC3"),
        std::make_tuple("89G, 56Y, 2S, 160T", "#", "89G, 56Y, 2S, 160T"),
        std::make_tuple("A,C,B", ",", "A,B,C")
    };
    for (int i = 0; i < 9; i++) {
        auto [arg0_s, arg1_s, expected] = table[i];

        if (!UdfTestHarness::ValidateUdf<StringVal, StringVal, StringVal>(
                Sort_Allele_List, arg0_s, arg1_s, expected
            )) {
            cout << "UDX sort_alleles(ss)->s failed:\n\t|" << arg0_s.ptr << "|\n\t|" << arg1_s.ptr
                 << "|\n\t|" << expected.ptr << "|\n";
            passing = false;
        }
    }

    return passing;
}

bool test__sort_list() {
    int passing = true;

    std::tuple<StringVal, StringVal, StringVal> table[8] = {
        std::make_tuple("B;C;A", ";", "A;B;C"),
        std::make_tuple("B;C;A", StringVal::null(), StringVal::null()),
        std::make_tuple(StringVal::null(), ";", StringVal::null()),
        std::make_tuple("B;C;A", "", "B;C;A"),
        std::make_tuple("BstarkCstarkA", "stark", "AstarkBstarkC"),
        std::make_tuple("Ok Bye;Hello, yes!", ";,", "Ok Bye;Hello, yes!"),
        std::make_tuple("Ok Bye;Hello, yes!", ";", "Hello, yes!;Ok Bye"),
        std::make_tuple("A,B,A,C", ",", "A,A,B,C")
    };
    for (int i = 0; i < 8; i++) {
        auto [arg0_s, arg1_s, expected] = table[i];

        if (!UdfTestHarness::ValidateUdf<StringVal, StringVal, StringVal>(
                Sort_List_By_Substring, arg0_s, arg1_s, expected
            )) {
            cout << "UDX sort_list(ss)->s failed:\n\t|" << arg0_s.ptr << "|\n\t|" << arg1_s.ptr
                 << "|\n\t|" << expected.ptr << "|\n";
            passing = false;
        }
    }

    return passing;
}

bool test__sort_list_set() {
    int passing = true;

    std::tuple<StringVal, StringVal, StringVal, StringVal> table[9] = {
        std::make_tuple("B;C;A", ";", ":", "A:B:C"),
        std::make_tuple(StringVal::null(), ";", ":", StringVal::null()),
        std::make_tuple("B;C;A", StringVal::null(), ":", StringVal::null()),
        std::make_tuple("B;C;A", ";", StringVal::null(), StringVal::null()),
        std::make_tuple("B;C;A", ";", "", "ABC"),
        std::make_tuple("B;C;A", "", ":", "B;C;A"),
        std::make_tuple("BstarkCstarkA", "stark", ":", "A:B:C"),
        std::make_tuple("Ok Bye;Hello, yes!", ";,", ":", " yes!:Hello:Ok Bye"),
        std::make_tuple("Ok Bye;Hello, yes!", ";", ":", "Hello, yes!:Ok Bye")
    };
    for (int i = 0; i < 9; i++) {
        auto [arg0_s, arg1_s, arg2_s, expected] = table[i];

        if (!UdfTestHarness::ValidateUdf<StringVal, StringVal, StringVal, StringVal>(
                Sort_List_By_Set, arg0_s, arg1_s, arg2_s, expected
            )) {
            cout << "UDX sort_list_set(sss)->s failed:\n\t|" << arg0_s.ptr << "|\n\t|" << arg1_s.ptr
                 << "|\n\t|" << arg2_s.ptr << "|\n\t|" << expected.ptr << "|\n";
            passing = false;
        }
    }

    return passing;
}

bool test__sort_list_unique() {
    int passing = true;

    std::tuple<StringVal, StringVal, StringVal> table[8] = {
        std::make_tuple("B;C;A", ";", "A;B;C"),
        std::make_tuple("B;C;A", StringVal::null(), StringVal::null()),
        std::make_tuple(StringVal::null(), ";", StringVal::null()),
        std::make_tuple("B;C;A", "", "B;C;A"),
        std::make_tuple("BstarkCstarkA", "stark", "AstarkBstarkC"),
        std::make_tuple("Ok Bye;Hello, yes!", ";,", "Ok Bye;Hello, yes!"),
        std::make_tuple("Ok Bye;Hello, yes!", ";", "Hello, yes!;Ok Bye"),
        std::make_tuple("A,B,A,C", ",", "A,B,C")
    };
    for (int i = 0; i < 8; i++) {
        auto [arg0_s, arg1_s, expected] = table[i];

        if (!UdfTestHarness::ValidateUdf<StringVal, StringVal, StringVal>(
                Sort_List_By_Substring_Unique, arg0_s, arg1_s, expected
            )) {
            cout << "UDX sort_list_unique(ss)->s failed:\n\t|" << arg0_s.ptr << "|\n\t|"
                 << arg1_s.ptr << "|\n\t|" << expected.ptr << "|\n";
            passing = false;
        }
    }

    return passing;
}

bool test__substr_range() {
    int passing = true;

    std::tuple<StringVal, StringVal, StringVal> table[9] = {
        std::make_tuple("SammySheep", "1..3", "Sam"),
        std::make_tuple(StringVal::null(), "1..3", StringVal::null()),
        std::make_tuple("SammySheep", StringVal::null(), StringVal::null()),
        std::make_tuple("SammySheep", "", StringVal::null()),
        std::make_tuple("", "1..3", StringVal::null()),
        std::make_tuple("SammySheep", "3..1;8..9;5", "maSeey"),
        std::make_tuple("123456789", "0..3", "123"),
        std::make_tuple("123456789", "7..12", "789"),
        std::make_tuple("ABC456", "1..2,5", "AB5")
    };
    for (int i = 0; i < 9; i++) {
        auto [arg0_s, arg1_s, expected] = table[i];

        if (!UdfTestHarness::ValidateUdf<StringVal, StringVal, StringVal>(
                Substring_By_Range, arg0_s, arg1_s, expected
            )) {
            cout << "UDX substr_range(ss)->s failed:\n\t|" << arg0_s.ptr << "|\n\t|" << arg1_s.ptr
                 << "|\n\t|" << expected.ptr << "|\n";
            passing = false;
        }
    }

    return passing;
}

bool test__nt_to_aa_position() {
    int passing                                                   = true;
    std::tuple<StringVal, StringVal, BigIntVal, IntVal> table[13] = {
        // "XXXATG"
        std::make_tuple("", "1..3", 3, IntVal::null()),
        std::make_tuple("4..6", "", 3, IntVal::null()),
        std::make_tuple("4..6", "1..3", 0, IntVal::null()),
        std::make_tuple(StringVal::null(), "1..3", 3, IntVal::null()),
        std::make_tuple("4..6", StringVal::null(), 3, IntVal::null()),
        std::make_tuple("4..6", "1..3", BigIntVal::null(), IntVal::null()),
        std::make_tuple("4..6", "1..3", 4, 1),
        // "XXXATGXTAG"
        std::make_tuple("4..6;8..10", "1..3;4..6", 7, IntVal::null()),
        std::make_tuple("4..6;8..10", "1..3;4..6", 9, 2),
        std::make_tuple("4..6;8..10", "1..3;4..6", 11, IntVal::null()),
        // "XXXATGXTAGCATTYG"
        std::make_tuple("4..6;8..10;11..16", "1..3;4..6;7..12", 11, 3),
        // Insertions cannot be returned with this method
        std::make_tuple("1..456;457..459;460..983", "31..486;486;487..1010", 458, IntVal::null()),
        std::make_tuple("1..456;457..459;460..983", "31..486;486;487..1010", 460, 163)

    };

    for (int i = 0; i < 13; i++) {
        auto [arg0_s, arg1_s, arg2_i, expected] = table[i];

        if (!UdfTestHarness::ValidateUdf<IntVal, StringVal, StringVal, BigIntVal>(
                NT_To_AA_Position, arg0_s, arg1_s, arg2_i, expected
            )) {
            cout << "UDX nt_to_aa_position(ssi)->i failed:\n\t|" << arg0_s.ptr << "|\n\t|"
                 << arg1_s.ptr << "|\n\t|" << arg2_i.val << "|\n\t|" << expected.val << "|\n";
            passing = false;
        }
    }

    return passing;
}

bool test__nt_to_cds_position() {
    int passing                                                   = true;
    std::tuple<StringVal, StringVal, BigIntVal, IntVal> table[13] = {
        // "XXXATG"
        std::make_tuple("", "1..3", 3, IntVal::null()),
        std::make_tuple("4..6", "", 3, IntVal::null()),
        std::make_tuple("4..6", "1..3", 0, IntVal::null()),
        std::make_tuple(StringVal::null(), "1..3", 3, IntVal::null()),
        std::make_tuple("4..6", StringVal::null(), 3, IntVal::null()),
        std::make_tuple("4..6", "1..3", BigIntVal::null(), IntVal::null()),
        std::make_tuple("4..6", "1..3", 4, 1),
        // "XXXATGXTAG"
        std::make_tuple("4..6;8..10", "1..3;4..6", 7, IntVal::null()),
        std::make_tuple("4..6;8..10", "1..3;4..6", 9, 5),
        std::make_tuple("4..6;8..10", "1..3;4..6", 11, IntVal::null()),
        // "XXXATGXTAGCATTYG"
        std::make_tuple("4..6;8..10;11..16", "1..3;4..6;7..12", 11, 7),
        // Insertions cannot be returned with this method
        std::make_tuple("1..456;457..459;460..983", "31..486;486;487..1010", 458, IntVal::null()),
        std::make_tuple("1..456;457..459;460..983", "31..486;486;487..1010", 460, 487)

    };

    for (int i = 0; i < 13; i++) {
        auto [arg0_s, arg1_s, arg2_i, expected] = table[i];

        if (!UdfTestHarness::ValidateUdf<IntVal, StringVal, StringVal, BigIntVal>(
                NT_To_CDS_Position, arg0_s, arg1_s, arg2_i, expected
            )) {
            cout << "UDX nt_to_cds_position(ssi)->i failed:\n\t|" << arg0_s.ptr << "|\n\t|"
                 << arg1_s.ptr << "|\n\t|" << arg2_i.val << "|\n\t|" << expected.val << "|\n";
            passing = false;
        }
    }

    return passing;
}


bool test__nt_position_to_codon() {
    int passing = true;

    std::tuple<StringVal, StringVal, StringVal, BigIntVal, StringVal> table[17] = {
        // Empty
        std::make_tuple("", "1..3", "ATG", 3, StringVal::null()),
        std::make_tuple("4..6", "", "ATG", 3, StringVal::null()),
        std::make_tuple("4..6", "1..3", "", 3, StringVal::null()),

        // Null Args
        std::make_tuple(StringVal::null(), "1..3", "ATG", 3, StringVal::null()),
        std::make_tuple("4..6", StringVal::null(), "ATG", 3, StringVal::null()),
        std::make_tuple("4..6", "1..3", "ATG", BigIntVal::null(), StringVal::null()),
        std::make_tuple("4..6", "1..3", StringVal::null(), 3, StringVal::null()),

        // Bounds
        // ori: XXXATG
        std::make_tuple("4..6", "1..3", "ATG", 0, StringVal::null()),
        // ori: XXXATGXTAG
        std::make_tuple("4..6;8..10", "1..3;4..6", "ATGTAG", 7, StringVal::null()),
        std::make_tuple("2..11", "21..30", "....................ATTCATTGCT", 1, StringVal::null()),

        // Codons
        // ori: XXXATG
        std::make_tuple("4..6", "1..3", "ATG", 4, "ATG"),
        // ori: XXXATGXTAG
        std::make_tuple("4..6;8..10", "1..3;4..6", "ATGTAG", 9, "TAG"),
        // ori: XXXATGXTAGCATTYN
        std::make_tuple("4..6;8..10;11..16", "1..3;4..6;7..12", "ATGTAGCATTYN", 8, "TAG"),
        std::make_tuple("4..6;8..10;11..16", "1..3;4..6;7..12", "ATGTAGCATTYN", 12, "CAT"),
        std::make_tuple("4..6;8..10;11..16", "1..3;4..6;7..12", "ATGTAGCATTYN", 16, "TYN"),
        // ori: tATTCATTGCT
        std::make_tuple("2..11", "21..30", "....................ATTCATTGCT", 2, "..A"),
        std::make_tuple("2..11", "21..30", "....................ATTCATTGCT", 3, "TTC"),
    };

    for (int i = 0; i < 17; i++) {
        auto [arg0_s, arg1_s, arg2_s, arg3_i, expected] = table[i];

        if (!UdfTestHarness::ValidateUdf<StringVal, StringVal, StringVal, StringVal, BigIntVal>(
                NT_Position_To_CDS_Codon, arg0_s, arg1_s, arg2_s, arg3_i, expected
            )) {
            cout << "UDX nt_position_to_codon(ssis)->i failed:\n\t|" << arg0_s.ptr << "|\n\t|"
                 << arg1_s.ptr << "|\n\t|" << arg2_s.ptr << "|\n\t|" << arg3_i.val << "|\n\t|"
                 << expected.ptr << "|\n";
            passing = false;
        }
    }

    return passing;
}

bool test__nt_position_to_codon_mutant() {
    int passing = true;

    std::tuple<StringVal, StringVal, StringVal, BigIntVal, StringVal, StringVal> table[19] = {
        // Empty
        std::make_tuple("", "1..3", "ATG", 3, "A", StringVal::null()),
        std::make_tuple("4..6", "", "ATG", 3, "A", StringVal::null()),
        std::make_tuple("4..6", "1..3", "", 3, "A", StringVal::null()),
        std::make_tuple("4..6", "1..3", "ATG", 3, "", StringVal::null()),

        // Null Args
        std::make_tuple(StringVal::null(), "1..3", "ATG", 3, "A", StringVal::null()),
        std::make_tuple("4..6", StringVal::null(), "ATG", 3, "A", StringVal::null()),
        std::make_tuple("4..6", "1..3", StringVal::null(), 3, "A", StringVal::null()),
        std::make_tuple("4..6", "1..3", "ATG", BigIntVal::null(), "A", StringVal::null()),
        std::make_tuple("4..6", "1..3", "ATG", 3, StringVal::null(), StringVal::null()),


        // Bounds
        // ori: XXXATG
        std::make_tuple("4..6", "1..3", "ATG", 0, "A", StringVal::null()),
        // ori: XXXATGXTAG
        std::make_tuple("4..6;8..10", "1..3;4..6", "ATGTAG", 7, "A", StringVal::null()),
        std::make_tuple(
            "2..11", "21..30", "....................ATTCATTGCT", 1, "A", StringVal::null()
        ),

        // Codons
        // ori: XXXATG
        std::make_tuple("4..6", "1..3", "ATG", 4, "t", "tTG"),
        // ori: XXXATGXTAG
        std::make_tuple("4..6;8..10", "1..3;4..6", "ATGTAG", 9, "C", "TCG"),
        // ori: XXXATGXTAGCATTYN
        std::make_tuple("4..6;8..10;11..16", "1..3;4..6;7..12", "ATGTAGCATTYN", 8, "G", "GAG"),
        std::make_tuple("4..6;8..10;11..16", "1..3;4..6;7..12", "ATGTAGCATTYN", 12, "N", "CNT"),
        std::make_tuple("4..6;8..10;11..16", "1..3;4..6;7..12", "ATGTAGCATTYN", 16, "a", "TYa"),
        // ori: tATTCATTGCT
        std::make_tuple("2..11", "21..30", "....................ATTCATTGCT", 2, "g", "..g"),
        std::make_tuple("2..11", "21..30", "....................ATTCATTGCT", 3, "a", "aTC"),
    };

    for (int i = 0; i < 19; i++) {
        auto [arg0_s, arg1_s, arg2_s, arg3_i, arg4_s, expected] = table[i];

        if (!UdfTestHarness::ValidateUdf<
                StringVal, StringVal, StringVal, StringVal, BigIntVal, StringVal>(
                NT_Position_To_CDS_Codon_Mutant, arg0_s, arg1_s, arg2_s, arg3_i, arg4_s, expected
            )) {
            cout << "UDX nt_position_to_codon(ssis)->i failed:\n\t|" << arg0_s.ptr << "|\n\t|"
                 << arg1_s.ptr << "|\n\t|" << arg2_s.ptr << "|\n\t|" << arg3_i.val << "|\n\t|"
                 << arg4_s.ptr << "|\n\t|" << expected.ptr << "|\n";
            passing = false;
        }
    }

    return passing;
}


bool test__nt_position_to_mutation_aa3() {
    int passing = true;

    std::tuple<StringVal, StringVal, StringVal, BigIntVal, StringVal, StringVal, StringVal>
        table[21] = {
            // Empty
            std::make_tuple("", "1..3", "ATG", 3, "A", "C", StringVal::null()),
            std::make_tuple("4..6", "", "ATG", 3, "A", "C", StringVal::null()),
            std::make_tuple("4..6", "1..3", "", 3, "A", "C", StringVal::null()),
            std::make_tuple("4..6", "1..3", "ATG", 3, "", "C", StringVal::null()),
            std::make_tuple("4..6", "1..3", "ATG", 3, "A", "", StringVal::null()),

            // Null Args
            std::make_tuple(StringVal::null(), "1..3", "ATG", 3, "A", "C", StringVal::null()),
            std::make_tuple("4..6", StringVal::null(), "ATG", 3, "A", "C", StringVal::null()),
            std::make_tuple("4..6", "1..3", StringVal::null(), 3, "A", "C", StringVal::null()),
            std::make_tuple("4..6", "1..3", "ATG", BigIntVal::null(), "A", "C", StringVal::null()),
            std::make_tuple("4..6", "1..3", "ATG", 3, StringVal::null(), "C", StringVal::null()),
            std::make_tuple("4..6", "1..3", "ATG", 3, "A", StringVal::null(), StringVal::null()),

            // Bounds
            // ori: XXXATG
            std::make_tuple("4..6", "1..3", "ATG", 0, "A", "C", StringVal::null()),
            // ori: XXXATGXTAG
            std::make_tuple("4..6;8..10", "1..3;4..6", "ATGTAG", 7, "A", "C", StringVal::null()),
            std::make_tuple(
                "2..11", "21..30", "....................ATTCATTGCT", 1, "A", "C", StringVal::null()
            ),

            // Codons
            // ori: XXXATG
            std::make_tuple("4..6", "1..3", "ATR", 6, "G", "A", "M1I"),
            // ori: XXXATGXTAG
            std::make_tuple("4..6;8..10", "1..3;4..6", "ATGTYG", 9, "C", "T", "S2L"),
            // ori: XXXATGXTAGCATTYN
            std::make_tuple(
                "4..6;8..10;11..16", "1..3;4..6;7..12", "ATGSCGCATTYN", 8, "C", "G", "P2A"
            ),
            std::make_tuple(
                "4..6;8..10;11..16", "1..3;4..6;7..12", "ATGTAGCMWTYN", 12, "C", "A", "P3H/Q"
            ),
            std::make_tuple(
                "4..6;8..10;11..16", "1..3;4..6;7..12", "ATGTAGCATTYK", 16, "g", "t", "L/S4F/S"
            ),
            // ori: tATTCATTGCT
            std::make_tuple(
                "2..11", "21..30", "....................ATTCATTGCT", 2, "g", ".", "~7."
            ),
            std::make_tuple(
                "2..11", "21..30", "....................AWTCATTGCT", 3, "t", "a", "F8I"
            ),
        };

    for (int i = 0; i < 21; i++) {
        auto [arg0_s, arg1_s, arg2_s, arg3_i, arg4_s, arg5_s, expected] = table[i];

        if (!UdfTestHarness::ValidateUdf<
                StringVal, StringVal, StringVal, StringVal, BigIntVal, StringVal, StringVal>(
                NT_Position_To_Mutation_AA3, arg0_s, arg1_s, arg2_s, arg3_i, arg4_s, arg5_s,
                expected
            )) {
            cout << "UDX og_position_to_mutation_aa3(ssiss)->i failed:\n\t|" << arg0_s.ptr
                 << "|\n\t|" << arg1_s.ptr << "|\n\t|" << arg2_s.ptr << "|\n\t|" << arg3_i.val
                 << "|\n\t|" << arg4_s.ptr << "|\n\t|" << arg5_s.ptr << "|\n\t|" << expected.ptr
                 << "|\n";
            passing = false;
        }
    }

    return passing;
}


bool test__to_aa() {
    int passing = true;

    std::tuple<StringVal, StringVal> table[7] = {
        std::make_tuple("ATGAGG---GGGTGGTAG", "MR-GW*"),
        std::make_tuple("", ""),
        std::make_tuple(StringVal::null(), StringVal::null()),
        std::make_tuple("ATGaggCC", "MR?"),
        std::make_tuple("...ATG.-~GGG", ".M~G"),
        std::make_tuple("AGGaagARG---GCGgcwGCRgcnzzz", "RKX-AAAA?"),
        std::make_tuple("..ATG..", "~~?")
    };
    for (int i = 0; i < 7; i++) {
        auto [arg0_s, expected] = table[i];

        if (!UdfTestHarness::ValidateUdf<StringVal, StringVal>(To_AA, arg0_s, expected)) {
            cout << "UDX to_aa(s)->s failed:\n\t|" << arg0_s.ptr << "|\n\t|" << expected.ptr
                 << "|\n";
            passing = false;
        }
    }

    return passing;
}

bool test__to_aa3() {
    int passing = true;

    std::tuple<StringVal, StringVal> table[10] = {
        std::make_tuple("ATGAGG---GGGTGGTAG", "MR-GW*"),
        std::make_tuple("", ""),
        std::make_tuple(StringVal::null(), StringVal::null()),
        std::make_tuple("ATGaggCC", "MR?"),
        std::make_tuple("...ATG.-~GGG", ".M~G"),
        std::make_tuple("AGGaagARG---GCGgcwGCRgcnzzz", "RK[K/R]-AAAA?"),
        std::make_tuple("..ATG..", "~~?"),
        std::make_tuple("ATGsCC", "M[A/P]"),
        std::make_tuple("GrN", "D/E/G"),
        std::make_tuple("GNy", "X")
    };
    for (int i = 0; i < 10; i++) {
        auto [arg0_s, expected] = table[i];

        if (!UdfTestHarness::ValidateUdf<StringVal, StringVal>(To_AA3, arg0_s, expected)) {
            cout << "UDX to_aa3(s)->s failed:\n\t|" << arg0_s.ptr << "|\n\t|" << expected.ptr
                 << "|\n";
            passing = false;
        }
    }

    return passing;
}


bool test__to_aa_mutant() {
    int passing = true;

    std::tuple<StringVal, StringVal, IntVal, StringVal> table[12] = {
        std::make_tuple("ATGAGG---GGGTGGTAG", "G", 1, "VR-GW*"),
        std::make_tuple("", "G", 1, "?"),
        std::make_tuple("ATG", "", 1, "M"),
        std::make_tuple(StringVal::null(), "G", 1, StringVal::null()),
        std::make_tuple("ATG", StringVal::null(), 1, StringVal::null()),
        std::make_tuple("ATG", "G", IntVal::null(), StringVal::null()),
        std::make_tuple("ATGaggCC", "ATG", 4, "MM?"),
        std::make_tuple("...ATG.-~GGG", "atgggg", 1, "MG~G"),
        std::make_tuple("ATGcagAGG", "GGG", 4, "MGR"),
        std::make_tuple("ATGcagAGG", "GGG", 0, "GMQR"),
        std::make_tuple("ATGcagAGG", "GGG", 10, "MQRG"),
        std::make_tuple("ATG", "ggATG", 2, "RM")
    };
    for (int i = 0; i < 12; i++) {
        auto [arg0_s, arg1_s, arg2_i, expected] = table[i];

        if (!UdfTestHarness::ValidateUdf<StringVal, StringVal, StringVal, IntVal>(
                To_AA_Mutant, arg0_s, arg1_s, arg2_i, expected
            )) {
            cout << "UDX to_aa_mutant(ssi)->s failed:\n\t|" << arg0_s.ptr << "|\n\t|" << arg1_s.ptr
                 << "|\n\t|" << arg2_i.val << "|\n\t|" << expected.ptr << "|\n";
            passing = false;
        }
    }

    return passing;
}

bool test__variant_hash() {
    int passing = true;

    std::tuple<StringVal, StringVal> table[9] = {
        std::make_tuple("", StringVal::null()),
        std::make_tuple(StringVal::null(), StringVal::null()),
        std::make_tuple("MNTQILVFALVASIPTNA", "f59e28966a24d41af41cd55ed00c08a4"),
        std::make_tuple("MNTQILVFALVASIPTNA :.-", "f59e28966a24d41af41cd55ed00c08a4"),
        std::make_tuple("..MNTQIL---VFA  LVASIPTNA:", "f59e28966a24d41af41cd55ed00c08a4"),
        std::make_tuple("MNTQILVFALVASIPTNA~", "3476ea2853c1363c8da186016063ef78"),
        std::make_tuple("mntqilvfalvasiptna", "f59e28966a24d41af41cd55ed00c08a4"),
        std::make_tuple("1", "c4ca4238a0b923820dcc509a6f75849b"),
        std::make_tuple("STARK", "a9106b6bc4ae581eb39418098a2891b4")
    };
    for (int i = 0; i < 9; i++) {
        auto [arg0_s, expected] = table[i];

        if (!UdfTestHarness::ValidateUdf<StringVal, StringVal>(variant_hash, arg0_s, expected)) {
            cout << "UDX variant_hash(s)->s failed:\n\t|" << arg0_s.ptr << "|\n\t|" << expected.ptr
                 << "|\n";
            passing = false;
        }
    }

    return passing;
}

bool test__sequence_diff() {
    bool passing = true;

    std::tuple<StringVal, StringVal, StringVal> table[10] = {
        std::make_tuple(StringVal("AGAGA"), StringVal("AGAGA"), StringVal(".....")),
        std::make_tuple(StringVal::null(), StringVal("AGAGA"), StringVal::null()),
        std::make_tuple(StringVal("AGAGA"), StringVal::null(), StringVal::null()),
        std::make_tuple(StringVal(""), StringVal("AGAGAGGGA"), StringVal::null()),
        std::make_tuple(StringVal("AGAGA"), StringVal(""), StringVal::null()),
        std::make_tuple(StringVal("AgAGA"), StringVal("AGAGA"), StringVal(".....")),
        std::make_tuple(StringVal("AaAGA"), StringVal("AGAGA"), StringVal(".G...")),
        std::make_tuple(StringVal("AGAGAG"), StringVal("AGAGA"), StringVal(".....")),
        std::make_tuple(StringVal("AGAGAGGAGAG"), StringVal("AGAGA"), StringVal(".....")),
        std::make_tuple(StringVal("AGAG"), StringVal("AGAGAGAGAA"), StringVal("...."))
    };

    for (int i = 0; i < 10; i++) {
        auto [arg0_s, arg1_s, expected] = table[i];
        if (!UdfTestHarness::ValidateUdf<StringVal, StringVal, StringVal>(
                Sequence_Diff, arg0_s, arg1_s, expected
            )) {
            cout << "UDX Sequence_Diff(s, s)->s failed:\n\t|" << arg0_s.ptr << "|\n\t|"
                 << arg1_s.ptr << "|\n\t|" << expected.ptr << "|\n";
            passing = false;
        }
    }
    return passing;
}

bool test__sequence_diff_nt() {
    bool passing = true;

    std::tuple<StringVal, StringVal, StringVal> table[18] = {
        std::make_tuple(StringVal("AGCTN-@"), StringVal("AGCTN-@"), StringVal("......?")),
        std::make_tuple(StringVal("AGAGA"), StringVal("AGAGA"), StringVal(".....")),
        std::make_tuple(StringVal::null(), StringVal::null(), StringVal::null()),
        std::make_tuple(StringVal::null(), StringVal("AGAGA"), StringVal::null()),
        std::make_tuple(StringVal("AGAGA"), StringVal::null(), StringVal::null()),
        std::make_tuple(StringVal(""), StringVal("AGAGAGGGA"), StringVal::null()),
        std::make_tuple(StringVal("AGAGA"), StringVal(""), StringVal::null()),
        std::make_tuple(StringVal("AgAGA"), StringVal("AGAGA"), StringVal(".....")),
        std::make_tuple(StringVal("AaAGA"), StringVal("AGAGA"), StringVal(".G...")),
        std::make_tuple(StringVal("AGAGAG"), StringVal("AGAGA"), StringVal(".....")),
        std::make_tuple(StringVal("AGAGAGGAGAG"), StringVal("AGAGA"), StringVal(".....")),
        std::make_tuple(StringVal("AGAG"), StringVal("AGAGAGAGAA"), StringVal("....")),
        std::make_tuple(
            StringVal("AAAAAAAAAAA"), StringVal("WMRDHVNSKYB"), StringVal(".......SKYB")
        ),
        std::make_tuple(
            StringVal("CCCCCCCCCCC"), StringVal("SMYBHVNWKRD"), StringVal(".......WKRD")
        ),
        std::make_tuple(
            StringVal("GGGGGGGGGGG"), StringVal("SKRBDVNWMYH"), StringVal(".......WMYH")
        ),
        std::make_tuple(
            StringVal("TTTTTTTTTTT"), StringVal("WKYBDHNSMRV"), StringVal(".......SMRV")
        ),
        std::make_tuple(StringVal("TTTTTTT"), StringVal("UKRBVHN"), StringVal("..R.V..")),
        std::make_tuple(StringVal("AAAA-A"), StringVal("AGA-AA"), StringVal(".G.-A."))
    };
    for (int i = 0; i < 18; i++) {
        auto [arg0_s, arg1_s, expected] = table[i];
        if (!UdfTestHarness::ValidateUdf<StringVal, StringVal, StringVal>(
                Sequence_Diff_NT, arg0_s, arg1_s, expected
            )) {
            cout << "UDX Sequence_Diff_NT(s, s)->s failed:\n\t|" << arg0_s.ptr << "|\n\t|"
                 << arg1_s.ptr << "|\n\t|" << expected.ptr << "|\n";
            passing = false;
        }
    }
    return passing;
}


bool test__calculate_entropy() {
    bool passing = true;

    std::tuple<StringVal, DoubleVal> table[8] = {
        std::make_tuple(StringVal::null(), DoubleVal::null()),
        std::make_tuple(StringVal(""), DoubleVal::null()),
        std::make_tuple(StringVal(" "), DoubleVal(0)),
        std::make_tuple(StringVal("S"), DoubleVal(0)),
        std::make_tuple(StringVal("SS"), DoubleVal(0)),
        std::make_tuple(StringVal("S  S"), DoubleVal(0)),
        std::make_tuple(StringVal("ABCDEFGH"), DoubleVal(3)),
        std::make_tuple(StringVal("AAaa"), DoubleVal(1))
    };
    for (int i = 0; i < 8; i++) {
        auto [arg_s, expected] = table[i];
        if (!UdfTestHarness::ValidateUdf<DoubleVal, StringVal>(
                Calculate_Entropy, arg_s, expected
            )) {
            cout << "UDX Calculate_Entropy(s)->s failed:\n\t|" << arg_s.ptr << "|\n\t|"
                 << expected.val << "|\n";
            passing = false;
        }
    }

    return passing;
}

bool test__tn_93() {
    bool passing = true;

    std::tuple<StringVal, StringVal, DoubleVal> table[9] = {
        std::make_tuple("ACGTX", "ACGAT", 0.32664338819911215),
        std::make_tuple("", "", DoubleVal::null()),
        std::make_tuple("ACGTX", "", DoubleVal::null()),
        std::make_tuple("ACGTX", StringVal::null(), DoubleVal::null()),
        std::make_tuple("ATCGATCGATCG", "ATCGATCGATCA", 0.10204780968478636),
        std::make_tuple("ACGTA", "ACGTA", 0.0),
        std::make_tuple("TTAAAAGCACGT", "TTAA--GTACGT", 0.13579170463426105),
        std::make_tuple("ACGTA", "acgtt", 0.23992356680997826),
        std::make_tuple("NNNN", "ACGT", DoubleVal::null()),
    };
    for (int i = 0; i < 9; i++) {
        auto [arg0_s, arg1_s, expected] = table[i];

        if (!UdfTestHarness::ValidateUdf<DoubleVal, StringVal, StringVal>(
                Tn_93_Distance, arg0_s, arg1_s, expected
            )) {
            cout << "UDX Tn_93_Distance(s, s)->d failed:\n\t|" << arg0_s.ptr << "|\n\t|"
                 << arg1_s.ptr << "|\n\t|" << expected.val << "|\n";
            passing = false;
        }
    }
    return passing;
}

bool test__tn_93_gamma() {
    bool passing = true;

    std::tuple<StringVal, StringVal, DoubleVal, DoubleVal> table[12] = {
        std::make_tuple("ACGTX", "ACGAT", 100.0, 0.32758930623668636),
        std::make_tuple("", "", 100.0, DoubleVal::null()),
        std::make_tuple("ACGTX", "", 100.0, DoubleVal::null()),
        std::make_tuple("ACGTX", StringVal::null(), 100.0, DoubleVal::null()),
        std::make_tuple("ATCGATCGATCG", "ATCGATCGATCA", 100.0, 0.10226233563817022),
        std::make_tuple("ATCGATCGATCG", "ATCGATCGATCA", 1.0, 0.12681159420289845),
        std::make_tuple("ATCGATCGATCG", "ATCGATCGATCA", 0.1, 1.5941755568827523),
        std::make_tuple("ATCGATCGATCG", "ATCGATCGATCA", 0.0, DoubleVal::null()),
        std::make_tuple("ATCGATCGATCG", "ATCGATCGATCA", DoubleVal::null(), DoubleVal::null()),
        std::make_tuple("TTAAAAGCACGT", "TTAA--GTACGT", 20.0, 0.1380107197790581),
        std::make_tuple("ACGTA", "acgtt", 3.0, 0.25598523599037293),
        std::make_tuple("ACGT", "NNNN", 1.0, DoubleVal::null()),
    };
    for (int i = 0; i < 12; i++) {
        auto [arg0_s, arg1_s, arg2_d, expected] = table[i];

        if (!UdfTestHarness::ValidateUdf<DoubleVal, StringVal, StringVal, DoubleVal>(
                Tn_93_Gamma, arg0_s, arg1_s, arg2_d, expected
            )) {
            cout << "UDX Tn_93_Gamma(s, s, d)->d failed:\n\t|" << arg0_s.ptr << "|\n\t|"
                 << arg1_s.ptr << "|\n\t|" << arg2_d.val << "|\n\t|" << expected.val << "|\n";
            passing = false;
        }
    }

    return passing;
}


int main(int argc, char **argv) {
    int passed = true;

    passed &= test__any_instr();
    passed &= test__complete_date();
    passed &= test__contains_element();
    passed &= test__contains_sym();
    passed &= test__cut_paste();
    passed &= test__cut_paste_out();
    passed &= test__deletion_events();
    passed &= test__hamming_distance();
    passed &= test__hamming_distance_pds();
    passed &= test__is_element();
    passed &= test__longest_deletion();
    passed &= test__mutation_list();
    passed &= test__mutation_list_range();
    passed &= test__mutation_list_gly();
    passed &= test__mutation_list_indel_gly();
    passed &= test__mutation_list_nt();
    passed &= test__nt_distance();
    passed &= test__nt_id();
    passed &= test__pcd();
    passed &= test__range_from_list();
    passed &= test__to_aa();
    passed &= test__to_aa_mutant();
    passed &= test__to_aa3();
    passed &= test__nt_to_cds_position();
    passed &= test__nt_position_to_codon();
    passed &= test__nt_position_to_codon_mutant();
    passed &= test__ending_in_saturday_str();
    passed &= test__ending_in_fornight_str();
    passed &= test__nt_to_aa_position();
    passed &= test__nt_position_to_mutation_aa3();
    passed &= test__sequence_diff();
    passed &= test__sequence_diff_nt();
    passed &= test__calculate_entropy();
    passed &= test__tn_93();
    passed &= test__tn_93_gamma();

    cerr << (passed ? "Tests passed." : "Tests failed.") << endl;
    return !passed;
}
