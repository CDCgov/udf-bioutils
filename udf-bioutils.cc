// Samuel S. Shepard, et al., CDC
// Impala user-defined functions for CDC biofinformatics.
// Relies on Cloudera headers being installed.
// Current version supports C++20

#include <algorithm>
#include <bitset>
#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/range/algorithm_ext/erase.hpp>
#include <boost/xpressive/xpressive.hpp>
#include <cctype>
#include <locale>
#include <openssl/md5.h>
#include <openssl/sha.h>
#include <set>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>

#include "boost/date_time/gregorian/gregorian.hpp"
#include <boost/exception/all.hpp>

#include "udf-bioutils.h"
#include "udx-inlines.h"
#include "udx-matrix.h"

#define PTM_GLY_WINDOW_SIZE 5


// Utility functions
// Compare alleles for two strings.
// TO-DO: revisit, likely outdated
bool comp_allele(std::string s1, std::string s2) {
    int x     = 0;
    int y     = 0;
    int index = 0;

    std::string buff1 = "";
    for (index = 0; index < s1.length(); index++) {
        if (isdigit(s1[index])) {
            buff1 += s1[index];
        } else if (!buff1.empty()) {
            break;
        }
    }
    std::istringstream(buff1) >> x;

    std::string buff2 = "";
    for (index = 0; index < s2.length(); index++) {
        if (isdigit(s2[index])) {
            buff2 += s2[index];
        } else if (!buff2.empty()) {
            break;
        }
    }
    std::istringstream(buff2) >> y;

    if (x < y) {
        return true;
    } else if (y < x) {
        return false;
    } else {
        return (s1 < s2);
    }
}

// We take a string of delimited values in a string and sort it in ascending
// order
IMPALA_UDF_EXPORT
StringVal Sort_List_By_Substring(
    FunctionContext *context, const StringVal &listVal, const StringVal &delimVal
) {
    if (listVal.is_null || delimVal.is_null) {
        return StringVal::null();
    }
    if (listVal.len == 0 || delimVal.len == 0) {
        return listVal;
    }

    std::string_view list((const char *)listVal.ptr, listVal.len);
    std::string_view delim((const char *)delimVal.ptr, delimVal.len);
    std::vector<std::string_view> tokens = split_by_substr(list, delim);

    if (tokens.size() == 0) {
        return listVal;
    } else {

        // Use the usual ascending sort
        std::sort(tokens.begin(), tokens.end());
        std::string s = "";

        s += tokens[0];
        for (auto i = tokens.begin() + 1; i < tokens.end(); ++i) {
            s += delim;
            s += *i;
        }

        return to_StringVal(context, s);
    }
}

// We take a string of delimited values in a string and sort it in ascending
// order
IMPALA_UDF_EXPORT
StringVal Range_From_List(
    FunctionContext *context, const StringVal &listVal, const StringVal &delimVal
) {
    if (listVal.is_null || delimVal.is_null) {
        return StringVal::null();
    }
    if (listVal.len == 0 || delimVal.len == 0) {
        return listVal;
    };

    std::string list((const char *)listVal.ptr, listVal.len);
    std::string delim((const char *)delimVal.ptr, delimVal.len);
    std::vector<int> tokens = split_set_by_substr(list, delim);
    std::sort(tokens.begin(), tokens.end());

    if (tokens.size() == 0) {
        return StringVal::null();
    } else {
        std::vector<int>::iterator it = tokens.begin();
        std::string s                 = std::to_string(*it);
        int previous                  = *it;
        ++it;
        int range = 0;

        while (it != tokens.end()) {
            if (*it == (previous + 1)) {
                range = 1;
            } else {
                if (range) {
                    range = 0;
                    s += ".." + std::to_string(previous);
                    s += delim + std::to_string(*it);
                } else {
                    s += delim + std::to_string(*it);
                }
            }
            previous = *it;
            ++it;
        }

        if (range) {
            s += ".." + std::to_string(previous);
        }

        return to_StringVal(context, s);
    }
}

// We take a string of delimited values in a string and sort it in ascending
// order
IMPALA_UDF_EXPORT
StringVal Sort_List_By_Substring_Unique(
    FunctionContext *context, const StringVal &listVal, const StringVal &delimVal
) {
    if (listVal.is_null || delimVal.is_null) {
        return StringVal::null();
    }
    if (listVal.len == 0 || delimVal.len == 0) {
        return listVal;
    };

    std::string list((const char *)listVal.ptr, listVal.len);
    std::string delim((const char *)delimVal.ptr, delimVal.len);
    std::vector<std::string> tokens = split_by_substr(list, delim);

    if (tokens.size() == 0) {
        if (list == delim) {
            return StringVal("");
        } else {
            return listVal;
        }
    } else {
        // Use the usual ascending sort
        std::sort(tokens.begin(), tokens.end());
        std::string s = tokens[0];
        for (std::size_t i = 1; i < tokens.size(); i++) {
            if (tokens[i] != tokens[i - 1]) {
                s += delim + tokens[i];
            }
        }

        return to_StringVal(context, s);
    }
}

IMPALA_UDF_EXPORT
StringVal Sort_List_By_Set(
    FunctionContext *context, const StringVal &listVal, const StringVal &delimVal,
    const StringVal &outDelimVal
) {
    if (listVal.is_null || delimVal.is_null || outDelimVal.is_null) {
        return StringVal::null();
    }
    if (listVal.len == 0 || delimVal.len == 0) {
        return listVal;
    };

    std::vector<std::string> tokens;
    std::string list((const char *)listVal.ptr, listVal.len);
    std::string delim((const char *)delimVal.ptr, delimVal.len);

    std::string odelim = "";
    if (outDelimVal.len > 0) {
        odelim.assign((const char *)outDelimVal.ptr, outDelimVal.len);
    }

    // Initialize positions
    std::string::size_type lastPos = list.find_first_not_of(delim, 0);
    std::string::size_type pos     = list.find_first_of(delim, lastPos);
    while (std::string::npos != pos || std::string::npos != lastPos) {
        tokens.push_back(list.substr(lastPos, pos - lastPos));
        lastPos = list.find_first_not_of(delim, pos);
        pos     = list.find_first_of(delim, lastPos);
    }

    // Use the usual ascending sort
    std::sort(tokens.begin(), tokens.end());
    std::string s = tokens[0];
    for (std::vector<std::string>::const_iterator i = tokens.begin() + 1; i < tokens.end(); ++i) {
        s += odelim + *i;
    }

    return to_StringVal(context, s);
}

// We take a string of delimited values in a string and sort it in ascending
// order
IMPALA_UDF_EXPORT
StringVal Sort_Allele_List(
    FunctionContext *context, const StringVal &listVal, const StringVal &delimVal
) {
    if (listVal.is_null || delimVal.is_null) {
        return StringVal::null();
    }
    if (listVal.len == 0 || delimVal.len == 0) {
        return listVal;
    };

    std::string list((const char *)listVal.ptr, listVal.len);
    std::string delim((const char *)delimVal.ptr, delimVal.len);
    std::vector<std::string> tokens = split_by_substr(list, delim);

    if (tokens.size() == 0) {
        return listVal;
    } else {
        // Use the usual ascending sort
        std::sort(tokens.begin(), tokens.end(), comp_allele);
        std::string s = tokens[0];
        for (std::vector<std::string>::const_iterator i = tokens.begin() + 1; i < tokens.end();
             ++i) {
            s += delim + *i;
        }

        return to_StringVal(context, s);
    }
}

IMPALA_UDF_EXPORT
BooleanVal Find_Set_In_String(
    FunctionContext *context, const StringVal &haystackVal, const StringVal &needlesVal
) {
    // check for nulls
    if (haystackVal.is_null || needlesVal.is_null) {
        return BooleanVal::null();
        // haystack and needles not null
    } else if (haystackVal.len == 0 || needlesVal.len == 0) {
        // Can't find something in nothing or vice-versa
        if (haystackVal.len != needlesVal.len) {
            return BooleanVal(false);
            // Special case that differs from instr
            // letting empty set be found in an empty string
        } else {
            return BooleanVal(true);
        }
        // haystack and needles are non-trivial
    } else {
        std::string haystack((const char *)haystackVal.ptr, haystackVal.len);
        std::string needles((const char *)needlesVal.ptr, needlesVal.len);
        return BooleanVal(haystack.find_first_of(needles) != std::string::npos);
    }
}

// We take codon(s) and translate it/them
IMPALA_UDF_EXPORT
StringVal To_AA(FunctionContext *context, const StringVal &ntsVal) {
    if (ntsVal.is_null) {
        return StringVal::null();
    }
    if (ntsVal.len == 0) {
        return ntsVal;
    };

    std::string bases((const char *)ntsVal.ptr, ntsVal.len);
    std::string residues = "";
    std::string aa       = "";
    std::string codon    = "";

    // Initialize positions
    long unsigned int N           = bases.length();
    long unsigned int R           = N % 3;
    long unsigned int codon_index = 0;

    for (codon_index = 0; codon_index + 2 < N; codon_index += 3) {
        // get codon and ignore case
        codon = bases.substr(codon_index, 3);
        for (std::string::size_type i = 0; i < 3; ++i) {
            codon[i] = toupper(codon[i]);
        }

        if (gc.contains(codon)) {
            aa = gc[codon];
        } else if (codon.find_first_of(".-~") != std::string::npos) {
            aa = "~";
        } else if (codon.find_first_not_of("ACGTURYSWKMBDHVN") != std::string::npos) {
            aa = "?";
        } else {
            aa = "X";
        }
        residues += aa;
    }

    if (R > 0) {
        residues += "?";
    }

    return to_StringVal(context, residues);
}

inline std::string codon_to_aa3(std::string codon, const std::size_t total_length) {
    std::string aa;

    // ignore case
    for (std::string::size_type i = 0; i < 3; ++i) {
        codon[i] = toupper(codon[i]);
    }

    if (gc.contains(codon)) {
        aa = gc[codon];
    } else if (codon.find_first_of(".-~") != std::string::npos) {
        aa = "~";
    } else if (codon.find_first_not_of("ACGTURYSWKMBDHVN") != std::string::npos) {
        aa = "?";
    } else if (gc3.contains(codon)) {
        if (total_length < 4) {
            aa = gc3[codon];
        } else {
            aa = "[" + gc3[codon] + "]";
        }
    } else {
        aa = "X";
    }
    return aa;
}

// We take codon(s) and translate it/them
IMPALA_UDF_EXPORT
StringVal To_AA3(FunctionContext *context, const StringVal &ntsVal) {
    if (ntsVal.is_null) {
        return StringVal::null();
    }
    if (ntsVal.len == 0) {
        return ntsVal;
    };

    std::string bases((const char *)ntsVal.ptr, ntsVal.len);
    std::string residues = "";
    std::string codon    = "";

    // Initialize positions
    long unsigned int N           = bases.length();
    long unsigned int R           = N % 3;
    long unsigned int codon_index = 0;

    for (codon_index = 0; codon_index + 2 < N; codon_index += 3) {
        residues += codon_to_aa3(bases.substr(codon_index, 3), bases.size());
    }

    if (R > 0) {
        residues += "?";
    }

    return to_StringVal(context, residues);
}

// Allows for mutating an allele before translation
IMPALA_UDF_EXPORT
StringVal To_AA_Mutant(
    FunctionContext *context, const StringVal &ntsVal, const StringVal &alleleVal, const IntVal &pos
) {
    if (ntsVal.is_null || alleleVal.is_null || pos.is_null) {
        return StringVal::null();
    }
    if (alleleVal.len == 0) {
        return To_AA(context, ntsVal);
    } else if (ntsVal.len == 0) {
        return To_AA(context, alleleVal);
    }

    std::string bases((const char *)ntsVal.ptr, ntsVal.len);
    std::string allele((const char *)alleleVal.ptr, alleleVal.len);

    if (pos.val < 1) {
        bases = allele + bases;
    } else if (pos.val > ntsVal.len) {
        bases = bases + allele;
    } else if ((pos.val + alleleVal.len - 1) > ntsVal.len) {
        bases.replace(pos.val - 1, ntsVal.len - pos.val + 1, allele);
    } else {
        bases.replace(pos.val - 1, allele.size(), allele);
    }

    // Copy sorted string to StringVal structure
    StringVal result(context, bases.size());
    memcpy(result.ptr, bases.c_str(), bases.size());
    return To_AA(context, result);
}

// Take the reverse complement of the nucleotide string
IMPALA_UDF_EXPORT
StringVal Rev_Complement(FunctionContext *context, const StringVal &ntsVal) {
    if (ntsVal.is_null) {
        return StringVal::null();
    } else if (ntsVal.len == 0) {
        return ntsVal;
    }

    StringVal revcomp(context, ntsVal.len);

    const int L = ntsVal.len;
    int r       = L - 1;
    for (int f = 0; f < L; ++f, --r) {
        revcomp.ptr[r] = RCM[ntsVal.ptr[f]];
    }

    return revcomp;
}

IMPALA_UDF_EXPORT
StringVal Complete_String_Date(FunctionContext *context, const StringVal &dateStr) {
    if (dateStr.is_null || dateStr.len == 0) {
        return StringVal::null();
    }

    std::string date((const char *)dateStr.ptr, dateStr.len);
    std::vector<std::string> tokens;
    boost::split(tokens, date, boost::is_any_of("-/."));

    std::string buffer = "";
    if (tokens.size() >= 3) {
        buffer = tokens[0] + "-" + tokens[1] + "-" + tokens[2];
    } else if (tokens.size() == 2) {
        buffer = tokens[0] + "-" + tokens[1] + "-01";
    } else if (tokens.size() == 1) {
        if (tokens[0].length() == 4) {
            buffer = tokens[0];
            buffer += "-01-01";
        } else if (tokens[0].length() == 6) {
            buffer = "20";
            buffer += tokens[0][0];
            buffer += tokens[0][1];
            buffer += "-";
            buffer += tokens[0][2];
            buffer += tokens[0][3];
            buffer += "-";
            buffer += tokens[0][4];
            buffer += tokens[0][5];
        } else {
            return StringVal::null();
        }
    } else {
        return StringVal::null();
    }

    return to_StringVal(context, buffer);
}

// Convert Grogorian Dates to the EPI (MMWR) Week
// See: https://wwwn.cdc.gov/nndss/document/MMWR_Week_overview.pdf
struct epiweek_t date_to_epiweek(boost::gregorian::date d) {
    // Boost starts with Sunday.
    int day_of_year = d.day_of_year();
    int weekday     = d.day_of_week();

    boost::gregorian::date start_date(d.year(), 1, 1);
    int start_weekday = start_date.day_of_week();
    boost::gregorian::date next_year_date(d.year() + 1, 1, 1);
    int next_year_weekday = next_year_date.day_of_week();

    // December & 29 - 31 &  Sun-Tues & Next year is Sun-Thu
    if (d.month() == 12 && d.day() > 28 && weekday < 3 && next_year_weekday < 4) {
        struct epiweek_t result = {d.year() + 1, 1};
        return result;
    }

    int epiweek = (day_of_year + (start_weekday - 1)) / 7;
    // Sunday, Monday, Tuesday, Wednesday
    if (start_weekday < 4) {
        epiweek++;
    }

    if (epiweek > 0) {
        struct epiweek_t result = {d.year(), epiweek};
        return result;
    } else {
        boost::gregorian::date last_year_date(d.year() - 1, 12, 31);
        return date_to_epiweek(last_year_date);
    }
}

DateVal ending_in_Saturday(boost::gregorian::date d) {
    // Boost starts with Sunday: 0 to 6
    boost::gregorian::date_duration days_until_saturday(6 - d.day_of_week());
    d += days_until_saturday;

    // Stored as the Days since the Unix Epoch
    return DateVal(d.day_number() - EPOCH_OFFSET);
}

// Calculation inspired by work from C. Paden
DateVal ending_in_Fortnight(boost::gregorian::date d, bool legacy_default_week) {
    // Final saturday should always be greater than d
    int diff = FINAL_SATURDAY - d.day_number();
    // Changes which week the 2-week period ends at
    if (legacy_default_week) {
        diff += 7;
    }
    boost::gregorian::date_duration days_until_fortnight(diff % 14);
    d += days_until_fortnight;

    // Stored as the Days since the Unix Epoch
    return DateVal(d.day_number() - EPOCH_OFFSET);
}


IMPALA_UDF_EXPORT
DateVal Date_Ending_In_Saturday_DATE(FunctionContext *context, const DateVal &dateVal) {
    if (dateVal.is_null) {
        return DateVal::null();
    } else {
        try {
            auto d = boost::gregorian::date(dateVal.val + EPOCH_OFFSET);
            return ending_in_Saturday(d);
        } catch (...) {
            return DateVal::null();
        }
    }
}

IMPALA_UDF_EXPORT
DateVal Date_Ending_In_Saturday_TS(FunctionContext *context, const TimestampVal &tsVal) {
    if (tsVal.is_null) {
        return DateVal::null();
    } else {
        try {
            boost::gregorian::date d(tsVal.date);
            return ending_in_Saturday(d);
        } catch (...) {
            return DateVal::null();
        }
    }
}

IMPALA_UDF_EXPORT
DateVal Date_Ending_In_Saturday_STR(FunctionContext *context, const StringVal &dateStr) {
    if (dateStr.is_null || dateStr.len == 0) {
        return DateVal::null();
    }
    std::string_view date((const char *)dateStr.ptr, dateStr.len);
    std::vector<std::string> tokens;
    boost::split(tokens, date, boost::is_any_of("-/."));

    try {
        int year, month, day;
        if (tokens.size() >= 3) {
            year  = std::stoi(tokens[0]);
            month = std::stoi(tokens[1]);
            day   = std::stoi(tokens[2]);
        } else {
            return DateVal::null();
        }
        boost::gregorian::date d(year, month, day);
        return ending_in_Saturday(d);
    } catch (...) {
        return DateVal::null();
    }
}

IMPALA_UDF_EXPORT
DateVal Fortnight_Date_STR(FunctionContext *context, const StringVal &dateStr) {
    return Fortnight_Date_Either_STR(context, dateStr, BooleanVal(true));
}

IMPALA_UDF_EXPORT
DateVal Fortnight_Date_TS(FunctionContext *context, const TimestampVal &tsVal) {
    return Fortnight_Date_Either_TS(context, tsVal, BooleanVal(true));
}

IMPALA_UDF_EXPORT
DateVal Fortnight_Date(FunctionContext *context, const DateVal &dateVal) {
    return Fortnight_Date_Either(context, dateVal, BooleanVal(true));
}


IMPALA_UDF_EXPORT
DateVal Fortnight_Date_Either(
    FunctionContext *context, const DateVal &dateVal, const BooleanVal &legacy_default_week
) {
    if (dateVal.is_null || legacy_default_week.is_null) {
        return DateVal::null();
    } else {
        try {
            auto d = boost::gregorian::date(dateVal.val + EPOCH_OFFSET);
            return ending_in_Fortnight(d, legacy_default_week.val);
        } catch (...) {
            return DateVal::null();
        }
    }
}

IMPALA_UDF_EXPORT
DateVal Fortnight_Date_Either_TS(
    FunctionContext *context, const TimestampVal &tsVal, const BooleanVal &legacy_default_week
) {
    if (tsVal.is_null || legacy_default_week.is_null) {
        return DateVal::null();
    } else {
        try {
            boost::gregorian::date d(tsVal.date);
            return ending_in_Fortnight(d, legacy_default_week.val);
        } catch (...) {
            return DateVal::null();
        }
    }
}

IMPALA_UDF_EXPORT
DateVal Fortnight_Date_Either_STR(
    FunctionContext *context, const StringVal &dateStr, const BooleanVal &legacy_default_week
) {
    if (dateStr.is_null || dateStr.len == 0 || legacy_default_week.is_null) {
        return DateVal::null();
    }

    std::string_view date((const char *)dateStr.ptr, dateStr.len);
    std::vector<std::string> tokens;
    boost::split(tokens, date, boost::is_any_of("-/."));

    try {
        int year, month, day;
        if (tokens.size() >= 3) {
            year  = std::stoi(tokens[0]);
            month = std::stoi(tokens[1]);
            day   = std::stoi(tokens[2]);
        } else {
            return DateVal::null();
        }
        boost::gregorian::date d(year, month, day);
        return ending_in_Fortnight(d, legacy_default_week.val);
    } catch (...) {
        return DateVal::null();
    }
}


IMPALA_UDF_EXPORT
IntVal Convert_Timestamp_To_EPI_Week(FunctionContext *context, const TimestampVal &tsVal) {
    return Convert_Timestamp_To_EPI_Week(context, tsVal, BooleanVal(false));
}

IMPALA_UDF_EXPORT
IntVal Convert_Timestamp_To_EPI_Week(
    FunctionContext *context, const TimestampVal &tsVal, const BooleanVal &yearFormat
) {
    if (tsVal.is_null || yearFormat.is_null) {
        return IntVal::null();
    }

    try {
        boost::gregorian::date d(tsVal.date);
        struct epiweek_t epi = date_to_epiweek(d);
        if (yearFormat.val) {
            return IntVal(epi.year * 100 + epi.week);
        } else {
            return IntVal(epi.week);
        }
    } catch (...) {
        return IntVal::null();
    }
}

IMPALA_UDF_EXPORT
IntVal Convert_String_To_EPI_Week(FunctionContext *context, const StringVal &dateStr) {
    return Convert_String_To_EPI_Week(context, dateStr, BooleanVal(false));
}

IMPALA_UDF_EXPORT
IntVal Convert_String_To_EPI_Week(
    FunctionContext *context, const StringVal &dateStr, const BooleanVal &yearFormat
) {
    if (dateStr.is_null || dateStr.len == 0 || yearFormat.is_null) {
        return IntVal::null();
    }
    std::string date((const char *)dateStr.ptr, dateStr.len);
    std::vector<std::string> tokens;
    boost::split(tokens, date, boost::is_any_of("-/."));

    try {
        int year, month, day;
        if (tokens.size() >= 3) {
            year  = std::stoi(tokens[0]);
            month = std::stoi(tokens[1]);
            day   = std::stoi(tokens[2]);
        } else {
            return IntVal::null();
        }

        boost::gregorian::date d(year, month, day);
        struct epiweek_t epi = date_to_epiweek(d);
        if (yearFormat.val) {
            return IntVal(epi.year * 100 + epi.week);
        } else {
            return IntVal(epi.week);
        }
    } catch (...) {
        return IntVal::null();
    }
}

IMPALA_UDF_EXPORT
IntVal NT_To_AA_Position(
    FunctionContext *context, const StringVal &oriMap, const StringVal &cdsMap,
    const BigIntVal &oriPos
) {
    IntVal v = NT_To_CDS_Position(context, oriMap, cdsMap, oriPos);
    if (v.is_null) {
        return IntVal::null();
    } else {
        // CDS position is 1-based and is always positive (see called function).
        // There is no integer divide_ceiling, so otherwise we would have to use
        // floats. This method avoid overflow on the original value.
        return IntVal((v.val - 1) / 3 + 1);
    }
}

IMPALA_UDF_EXPORT
IntVal NT_To_CDS_Position(
    FunctionContext *context, const StringVal &oriMap, const StringVal &cdsMap,
    const BigIntVal &oriPos
) {
    // Check for empty/null states but importantly ensures ori position is strictly positive
    if (oriMap.is_null || oriMap.len == 0 || cdsMap.is_null || cdsMap.len == 0 || oriPos.is_null ||
        oriPos.val < 1) {
        return IntVal::null();
    }

    std::string_view ori((const char *)oriMap.ptr, oriMap.len);
    std::string_view cds((const char *)cdsMap.ptr, cdsMap.len);
    int ori_pos = oriPos.val;

    std::vector<std::string_view> ori_tokens = split_by_delims(ori, ";");
    std::vector<std::string_view> cds_tokens = split_by_delims(cds, ";");

    if (ori_tokens.size() != cds_tokens.size()) {
        return IntVal::null();
    }

    for (int i = 0; i < ori_tokens.size(); i++) {
        std::vector<int> ori_range = split_int_by_substr(ori_tokens[i], "..");

        // We must be in range
        if (ori_range.size() == 2 && ori_range[0] <= ori_pos && ori_pos <= ori_range[1]) {
            // The CDS is a subset of the nt sequence, so only co-ranges may be considered
            std::vector<int> cds_range = split_int_by_substr(cds_tokens[i], "..");
            if (cds_range.size() == 2) {
                int offset = ori_pos - ori_range[0];
                // We only care about the first matching range found, but theoretically CDS
                // could have overlapping exons.
                return IntVal(cds_range[0] + offset);
            }
        }
    }

    // Ultimately not found
    return IntVal::null();
}


IMPALA_UDF_EXPORT
StringVal NT_Position_To_CDS_Codon_Mutant(
    FunctionContext *context, const StringVal &oriMap, const StringVal &cdsMap,
    const StringVal &cdsAlignment, const BigIntVal &oriPos, const StringVal &allele
) {
    if (cdsAlignment.is_null || cdsAlignment.len == 0 || allele.is_null || allele.len == 0) {
        return StringVal::null();
    }

    IntVal v = NT_To_CDS_Position(context, oriMap, cdsMap, oriPos);
    if (v.is_null) {
        return StringVal::null();
    } else {
        // Zero-based index for String. Codon position is also zero-based.
        int cds_index      = v.val - 1;
        int codon_position = cds_index % 3;
        int codon_index    = cds_index - codon_position;

        // Last codon index is +2 and for length +1
        if (cdsAlignment.len < codon_index + 3) {
            return StringVal::null();
        }

        std::string codon((const char *)cdsAlignment.ptr + codon_index, 3);
        codon[codon_position] = allele.ptr[0];

        return to_StringVal(context, codon);
    }
}

IMPALA_UDF_EXPORT
StringVal NT_Position_To_Mutation_AA3(
    FunctionContext *context, const StringVal &oriMap, const StringVal &cdsMap,
    const StringVal &cdsAlignment, const BigIntVal &oriPos, const StringVal &major_allele,
    const StringVal &minor_allele
) {
    if (cdsAlignment.is_null || cdsAlignment.len == 0 || major_allele.is_null ||
        major_allele.len == 0 || minor_allele.is_null || minor_allele.len == 0) {
        return StringVal::null();
    }

    IntVal v = NT_To_CDS_Position(context, oriMap, cdsMap, oriPos);
    if (v.is_null) {
        return StringVal::null();
    } else {
        // Zero-based index codon_position. Codon_index is WRT the whole string.
        // However, AA_position is 1-based.
        int cds_index      = v.val - 1;
        int codon_position = cds_index % 3;
        int codon_index    = cds_index - codon_position;
        int aa_position    = cds_index / 3 + 1;

        // Last codon index is +2 and for length +1
        if (cdsAlignment.len < codon_index + 3) {
            return StringVal::null();
        }

        std::string codon((const char *)cdsAlignment.ptr + codon_index, 3);
        codon[codon_position] = major_allele.ptr[0];

        std::string buffer = codon_to_aa3(codon, 3);
        append_int(buffer, aa_position);
        codon[codon_position] = minor_allele.ptr[0];
        buffer += codon_to_aa3(codon, 3);

        return to_StringVal(context, buffer);
    }
}

IMPALA_UDF_EXPORT
StringVal NT_Position_To_CDS_Codon(
    FunctionContext *context, const StringVal &oriMap, const StringVal &cdsMap,
    const StringVal &cdsAlignment, const BigIntVal &oriPos
) {
    if (cdsAlignment.is_null || cdsAlignment.len == 0) {
        return StringVal::null();
    }

    IntVal v = NT_To_CDS_Position(context, oriMap, cdsMap, oriPos);
    if (v.is_null) {
        return StringVal::null();
    } else {
        // Zero-based index for String
        int cds_index   = v.val - 1;
        int codon_index = cds_index - (cds_index % 3);

        // Last codon index is +2 and for length +1
        if (cdsAlignment.len < codon_index + 3) {
            return StringVal::null();
        }

        // Safety: we checked the length above
        return StringVal::CopyFrom(context, cdsAlignment.ptr + codon_index, 3);
    }
}

IMPALA_UDF_EXPORT StringVal
Substring_By_Range(FunctionContext *context, const StringVal &sequence, const StringVal &rangeMap) {
    if (sequence.is_null || sequence.len == 0 || rangeMap.is_null || rangeMap.len == 0) {
        return StringVal::null();
    }

    std::string_view seq((const char *)sequence.ptr, sequence.len);
    std::string_view map((const char *)rangeMap.ptr, rangeMap.len);

    std::string buffer = "";
    const int L        = seq.length();

    std::vector<std::string_view> tokens = split_by_delims(map, ";,");
    for (const auto &t : tokens) {
        std::vector<int> range = split_int_by_substr(t, "..");
        const int R            = range.size();
        if (R > 1) {
            int a = range[0] - 1;
            if (a >= L) {
                a = L - 1;
            } else if (a < 0) {
                a = 0;
            }

            int b = range[1] - 1;
            if (b >= L) {
                b = L - 1;
            } else if (b < 0) {
                b = 0;
            }

            if (a <= b) {
                buffer += seq.substr(a, b - a + 1);
                // b < a
            } else {
                for (int j = a; j >= b; j--) {
                    buffer += seq[j];
                }
            }
        } else if (R == 1) {
            const int x = range[0] - 1;
            if (x < L && x >= 0) {
                buffer += seq[x];
            }
            // R == 0
        } else {
            return StringVal::null();
        }
    }

    return to_StringVal(context, buffer);
}

IMPALA_UDF_EXPORT
StringVal Cut_Paste(
    FunctionContext *context, const StringVal &my_string, const StringVal &delim,
    const StringVal &range_map
) {
    return Cut_Paste_Output(context, my_string, delim, range_map, StringVal::null());
}

IMPALA_UDF_EXPORT StringVal Cut_Paste_Output(
    FunctionContext *context, const StringVal &my_string, const StringVal &delim,
    const StringVal &range_map, const StringVal &out_delim
) {
    if (my_string.is_null || delim.is_null || range_map.is_null) {
        return StringVal::null();
    } else if (my_string.len == 0 || delim.len == 0 || range_map.len == 0) {
        return my_string;
    }


    std::string_view s((const char *)my_string.ptr, my_string.len);
    std::string_view d((const char *)delim.ptr, delim.len);
    std::string_view map((const char *)range_map.ptr, range_map.len);

    std::string_view od;
    if (out_delim.is_null) {
        od = d;
    } else {
        od = std::string_view((const char *)out_delim.ptr, out_delim.len);
    }

    // If we don't have the delimeter, return the whole string
    if (s.find(d) == std::string::npos) {
        return my_string;
    }

    std::vector<std::string_view> tokens = split_by_substr(s, d);
    std::vector<std::string_view> ranges = split_by_delims(map, ",;");
    std::string buffer                   = "";
    const int L                          = tokens.size();

    for (const auto &r : ranges) {
        std::vector<int> range;
        if (r.find("-") != std::string::npos) {
            range = split_int_by_substr(r, "-");
        } else {
            range = split_int_by_substr(r, "..");
        }
        const int R = range.size();

        // Multi-value range
        if (R == 2) {
            int a = range[0] - 1;
            int b = range[1] - 1;
            if (a >= L || a < 0 || b >= L || b < 0) {
                continue;
            }

            if (a <= b) {
                for (int i = a; i <= b; i++) {
                    buffer += tokens[i];
                    buffer += od;
                }
            } else {
                // b < a
                for (int j = a; j >= b; j--) {
                    buffer += tokens[j];
                    buffer += od;
                }
            }
        } else if (R == 1) {
            const int x = range[0] - 1;
            if (x >= L || x < 0) {
                continue;
            }

            buffer += tokens[x];
            buffer += od;
        } else {
            // R == 0 OR R > 2
            return StringVal::null();
        }
    }

    if (buffer.length() > od.length()) {
        buffer.erase(buffer.length() - od.length());
    }

    return to_StringVal(context, buffer);
}

// Create a mutation list from two aligned strings
IMPALA_UDF_EXPORT
StringVal Mutation_List_Strict(
    FunctionContext *context, const StringVal &sequence1, const StringVal &sequence2
) {
    if (sequence1.is_null || sequence2.is_null || sequence1.len == 0 || sequence2.len == 0) {
        return StringVal::null();
    }

    std::size_t length = sequence1.len;
    if (sequence2.len < sequence1.len) {
        length = sequence2.len;
    }

    const unsigned char *seq1 = sequence1.ptr;
    const unsigned char *seq2 = sequence2.ptr;
    std::string buffer        = "";
    unsigned char s1          = ' ';
    unsigned char s2          = ' ';

    for (std::size_t i = 0; i < length; i++) {
        if (seq1[i] != seq2[i]) {
            s1 = toupper(seq1[i]);
            s2 = toupper(seq2[i]);
            if (s1 != s2 && s1 != '.' && s2 != '.') {
                buffer += ", ";
                buffer += s1;
                append_int(buffer, (i + 1));
                buffer += s2;
            }
        }
    }

    if (buffer.length() > 2) {
        buffer.erase(0, 2);
    }

    return to_StringVal(context, buffer);
}

// Create a mutation list from two aligned strings
IMPALA_UDF_EXPORT
StringVal Mutation_List_PDS(
    FunctionContext *context, const StringVal &sequence1, const StringVal &sequence2,
    const StringVal &pairwise_delete_set
) {
    if (sequence1.is_null || sequence2.is_null || pairwise_delete_set.is_null ||
        sequence1.len == 0 || sequence2.len == 0) {
        return StringVal::null();
    }

    std::size_t length = sequence1.len;
    if (sequence2.len < sequence1.len) {
        length = sequence2.len;
    }

    const unsigned char *seq1 = sequence1.ptr;
    const unsigned char *seq2 = sequence2.ptr;
    std::string buffer        = "";
    unsigned char s1          = ' ';
    unsigned char s2          = ' ';

    std::array<bool, 256> valid;
    valid.fill(true);
    if (pairwise_delete_set.len > 0) {
        const unsigned char *dset = pairwise_delete_set.ptr;
        for (std::size_t i = 0; i < pairwise_delete_set.len; i++) {
            valid[dset[i]] = false;
        }
    }


    for (std::size_t i = 0; i < length; i++) {
        if (seq1[i] != seq2[i]) {
            s1 = to_const_upper(seq1[i]);
            s2 = to_const_upper(seq2[i]);
            if (s1 != s2 && valid[s1] && valid[s2]) {
                buffer += ", ";
                buffer += s1;
                append_int(buffer, (i + 1));
                buffer += s2;
            }
        }
    }

    if (buffer.length() > 2) {
        buffer.erase(0, 2);
    }

    return to_StringVal(context, buffer);
}

IMPALA_UDF_EXPORT
StringVal Mutation_List_Strict_Range(
    FunctionContext *context, const StringVal &sequence1, const StringVal &sequence2,
    const StringVal &rangeMap
) {
    if (sequence1.is_null || sequence2.is_null || rangeMap.is_null) {
        return StringVal::null();
    }
    if (sequence1.len == 0 || sequence2.len == 0 || rangeMap.len == 0) {
        return StringVal::null();
    };

    std::size_t length = sequence1.len;
    if (sequence2.len < sequence1.len) {
        length = sequence2.len;
    }

    std::string seq1((const char *)sequence1.ptr, sequence1.len);
    std::string seq2((const char *)sequence2.ptr, sequence2.len);
    std::string map((const char *)rangeMap.ptr, rangeMap.len);

    int x, a, b;
    int L = length;
    std::vector<int> sites;
    std::vector<std::string> tokens;
    boost::split(tokens, map, boost::is_any_of(";,"));
    for (int i = 0; i < tokens.size(); i++) {
        if (tokens[i].find("..") != std::string::npos) {
            std::vector<std::string> range = split_by_substr(tokens[i], "..");
            if (range.size() == 0) {
                return StringVal::null();
            }

            try {
                a = std::stoi(range[0]) - 1;
                b = std::stoi(range[1]) - 1;
            } catch (...) {
                return StringVal::null();
            }

            if (b >= L) {
                b = L - 1;
            }
            if (a >= L) {
                a = L - 1;
            }
            if (a < 0) {
                a = 0;
            }
            if (b < 0) {
                b = 0;
            }

            if (a <= b) {
                for (int j = a; j <= b; j++) {
                    sites.push_back(j);
                }
            } else {
                for (int j = a; j >= b; j--) {
                    sites.push_back(j);
                }
            }
        } else {
            try {
                x = std::stoi(tokens[i]) - 1;
            } catch (...) {
                return StringVal::null();
            }

            if (x < L && x >= 0) {
                sites.push_back(x);
            }
        }
    }

    int pos            = 0;
    std::string buffer = "";
    for (const auto &i : sites) {
        if (i < length && i > -1) {
            seq1[i] = toupper(seq1[i]);
            seq2[i] = toupper(seq2[i]);
            if (seq1[i] != seq2[i]) {
                if (seq1[i] != '.' && seq2[i] != '.') {
                    pos = i + 1;
                    if (buffer.length() > 0) {
                        buffer += std::string(", ") + seq1[i] + std::to_string(pos) + seq2[i];
                    } else {
                        buffer += seq1[i] + std::to_string(pos) + seq2[i];
                    }
                }
            }
        }
    }

    return to_StringVal(context, buffer);
}

// Create a mutation list from two aligned strings
// Add Glycosylation detection
IMPALA_UDF_EXPORT
StringVal Mutation_List_Strict_GLY(
    FunctionContext *context, const StringVal &sequence1, const StringVal &sequence2
) {
    if (sequence1.is_null || sequence2.is_null) {
        return StringVal::null();
    }
    if (sequence1.len == 0 || sequence2.len == 0) {
        return StringVal::null();
    };

    std::size_t length = sequence1.len;
    if (sequence2.len < sequence1.len) {
        length = sequence2.len;
    }

    std::string seq1((const char *)sequence1.ptr, sequence1.len);
    std::string seq2((const char *)sequence2.ptr, sequence2.len);
    std::string buffer = "";

    int add_gly  = 0;
    int loss_gly = 0;
    int is_mut   = 0;

    for (std::size_t i = 0; i < length; i++) {
        is_mut   = 0;
        add_gly  = 0;
        loss_gly = 0;


        seq1[i] = toupper(seq1[i]);
        seq2[i] = toupper(seq2[i]);
        if (seq1[i] != seq2[i]) {
            if (seq1[i] != '.' && seq2[i] != '.' && seq1[i] != '-' && seq2[i] != '-') {
                is_mut = 1;

                // GLYCOSYLATION ADD
                // ~N <= N
                if (seq2[i] == 'N') {
                    // CHECK: .[^P][ST]
                    if ((i + 2) < length && seq2[i + 1] != 'P' &&
                        (seq2[i + 2] == 'T' || seq2[i + 2] == 'S')) {
                        add_gly = 1;
                    }
                }

                // P => ~P
                if (!add_gly && seq1[i] == 'P') {
                    // CHECK: N.[ST]
                    if ((i + 1) < length && i >= 1 && seq2[i - 1] == 'N' &&
                        (seq2[i + 1] == 'T' || seq2[i + 1] == 'S')) {
                        add_gly = 1;
                    }
                }

                // ~[ST] && [ST]
                if (!add_gly && seq1[i] != 'S' && seq1[i] != 'T' &&
                    (seq2[i] == 'S' || seq2[i] == 'T')) {
                    // CHECK: N[^P].
                    if (i >= 2 && seq2[i - 2] == 'N' && seq2[i - 1] != 'P') {
                        add_gly = 1;
                    }
                }


                // GLYCOSYLATION LOSS
                // N => ~N
                if (seq1[i] == 'N') {
                    // CHECK: .[^P][ST]
                    if ((i + 2) < length && seq1[i + 1] != 'P' &&
                        (seq1[i + 2] == 'T' || seq1[i + 2] == 'S')) {
                        loss_gly = 1;
                    }
                }

                // ~P <= P
                if (!loss_gly && seq2[i] == 'P') {
                    // CHECK: N.[ST]
                    if ((i + 1) < length && i >= 1 && seq1[i - 1] == 'N' &&
                        (seq1[i + 1] == 'T' || seq1[i + 1] == 'S')) {
                        loss_gly = 1;
                    }
                }

                // [ST] && ~[ST]
                if (!loss_gly && seq2[i] != 'S' && seq2[i] != 'T' &&
                    (seq1[i] == 'S' || seq1[i] == 'T')) {
                    // CHECK: N[^P].
                    if (i >= 2 && seq1[i - 2] == 'N' && seq1[i - 1] != 'P') {
                        loss_gly = 1;
                    }
                }
            }
        }

        // Deletions that cause changes in glycosylation

        if (seq1[i] != '-' && seq2[i] == '-') {
            is_mut = 0;

            // N.[^P][ST] -> N-[^P][ST]
            if (0 <= (i - 1) && (i + 2) < length &&           // checking for length
                seq2[i - 1] == 'N' &&                         // -2 position
                seq2[i + 1] != 'P' &&                         // -1 position
                (seq2[i + 2] == 'S' || seq2[i + 2] == 'T')) { // 0 position
                add_gly = 1;
            }

            // N[^P].[ST] -> N[^P]-[ST]
            if (0 <= (i - 2) && (i + 1) < length &&           // checking for length
                seq2[i - 2] == 'N' &&                         // -2 position
                seq2[i - 1] != 'P' &&                         // -1 position
                (seq2[i + 1] == 'S' || seq2[i + 1] == 'T')) { // 0 position
                add_gly = 1;
            }

            // N[^P][ST] -> -[^P][ST]
            if (0 <= i && (i + 2) < length &&                 // checking for length
                seq1[i] == 'N' &&                             // -2 position
                seq1[i + 1] != 'P' &&                         // -1 position
                (seq1[i + 2] == 'S' || seq1[i + 2] == 'T')) { // 0 position
                loss_gly = 1;
            }

            // N[^P][ST] -> N-[ST]
            if (0 <= (i - 1) && (i + 1) < length &&           // checking for length
                seq1[i - 1] == 'N' &&                         // -2 position
                seq1[i] != 'P' &&                             // -1 position
                (seq1[i + 1] == 'S' || seq1[i + 1] == 'T')) { // 0 position
                loss_gly = 1;
            }

            // N[^P][ST] -> N[^P]-
            if (0 <= (i - 2) && i < length &&         // checking for length
                seq1[i - 2] == 'N' &&                 // -2 position
                seq1[i - 1] != 'P' &&                 // -1 position
                (seq1[i] == 'S' || seq1[i] == 'T')) { // 0 position
                loss_gly = 1;
            }
        }

        // Check if the mutation should be added to the buffer for final output
        // Will only trigger for (1) all mutations or (2) deletions that
        // specifically causes change in glycosylation site.
        if (is_mut || ((add_gly || loss_gly) && !is_mut)) {
            if (buffer.length() > 0) {
                buffer += ", ";
                buffer += seq1[i];
                buffer += boost::lexical_cast<std::string>(i + 1);
                buffer += seq2[i];
            } else {
                buffer = seq1[i] + boost::lexical_cast<std::string>(i + 1) + seq2[i];
            }


            if (add_gly && loss_gly) {
                buffer += "(CHO+/-)";
            }
            else if (add_gly) {
                buffer += "(CHO+)";
            }
            else if (loss_gly) {
                buffer += "(CHO-)";
            }
        }
    }

    return to_StringVal(context, buffer);
}

IMPALA_UDF_EXPORT
StringVal Mutation_List_Indel_GLY(
    FunctionContext *context, const StringVal &seq1_, const StringVal &seq2_
) {
    if (seq1_.is_null || seq2_.is_null || seq1_.len == 0 || seq2_.len == 0) {
        return StringVal::null();
    }

    using namespace boost::xpressive;

    static const std::string p("(?i:N-*[A-OQ-Z]-*[ST]$)"); // check
    static const sregex r = sregex::compile(p);

    const std::string seq1((const char *)seq1_.ptr, seq1_.len);
    const std::string seq2((const char *)seq2_.ptr, seq2_.len);

    std::string buffer;
    const size_t max_i = std::min(seq1.size(), seq2.size());
    smatch sm1, sm2;

    // Initialize a 2-dimensional array for storing ptm information.
    std::vector<std::array<std::bitset<PTM_GLY_WINDOW_SIZE>, 2>> is_motif(max_i);

    std::bitset<PTM_GLY_WINDOW_SIZE> cur_ptm(1);
    bool has_ptm;
    char prev_char;

    for (size_t i = 0; i < max_i; i++) {
        has_ptm = false;

        // Look for serines or threonines
        if (toupper(seq1[i]) != 'S' && toupper(seq1[i]) != 'T' && toupper(seq2[i]) != 'S' &&
            toupper(seq2[i]) != 'T') {
            continue;
        }

        // Substring the sequence around PTM site
        const size_t sub_pos    = std::max<int>(0, i - PTM_GLY_WINDOW_SIZE + 1);
        const size_t sub_length = std::min<int>(PTM_GLY_WINDOW_SIZE, i - sub_pos + 1);
        ;
        std::string seq1_sub = seq1.substr(sub_pos, sub_length);
        std::string seq2_sub = seq2.substr(sub_pos, sub_length);

        // Does the motif match in seq1?
        bool seq1_m = regex_search(seq1_sub, sm1, r);
        bool seq2_m = regex_search(seq2_sub, sm2, r);


        // See if there's a difference in PTM recognition between seq1 and seq2
        if (seq1_m && !seq2_m) {
            has_ptm = true;

            // Mark the positions associated with PTM motif
            for (size_t j = i - sm1[0].length() + 1; j <= i; j++) {
                // Check if the mutation is resonsible for PTM
                for (int k = sm1[0].first - seq1_sub.begin(); k < sm1[0].second - seq1_sub.begin();
                     k++) {
                    if (seq1_sub[k] == seq2_sub[k] || seq1_sub[k] == '.' || seq2_sub[k] == '.') {
                        continue;
                    }

                    // Temporarily mutate position of interest
                    // to see if they match PTM motif
                    prev_char   = seq1_sub[k];
                    seq1_sub[k] = seq2_sub[k];

                    // Does seq1 have a mutation that causes loss of PTM
                    // recognition?
                    if (!regex_search(seq1_sub, r)) {
                        is_motif[k + sub_pos][0] |= cur_ptm;
                    }
                    seq1_sub[k] = prev_char;
                }
            }
        }

        // See if there's a difference in PTM recognition between seq1 and seq2
        if (seq2_m && !seq1_m) {
            has_ptm = true;

            // Mark the positions associated with PTM motif
            for (size_t j = i - sm2[0].length() + 1; j <= i; j++)
                // Check if the mutation is resonsible for PTM
                for (int k = sm2[0].first - seq2_sub.begin(); k < sm2[0].second - seq2_sub.begin();
                     k++) {
                    if (seq1_sub[k] == seq2_sub[k] || seq1_sub[k] == '.' || seq2_sub[k] == '.') {
                        continue;
                    }

                    // Temporarily mutate position of interest
                    // to see if they match PTM motif
                    prev_char   = seq2_sub[k];
                    seq2_sub[k] = seq1_sub[k];

                    // Does seq2 have a mutation that causes loss of PTM
                    // recognition?
                    if (!regex_search(seq2_sub, r)) {
                        is_motif[k + sub_pos][1] |= cur_ptm;
                    }
                    seq2_sub[k] = prev_char;
                }
        }

        // Increment for the next ptm
        if (has_ptm) {
            cur_ptm = cur_ptm << 1;
        }
    }


    // Annotate mutations
    for (size_t i = 0; i < max_i; i++) {
        // Annotate the mutation
        if (// Checks if mutation is not indel
            ((toupper(seq1[i]) != toupper(seq2[i]) &&
              isalpha(seq1[i]) && isalpha(seq2[i])) ||

            // If mutation is indel, check if it changes recognition sequence
             (toupper(seq1[i]) != toupper(seq2[i]) &&
              is_motif[i][0] != is_motif[i][1]))) {

            if (!buffer.empty()) {
                buffer.append(", ");
            }
            buffer.push_back(isalpha(seq1[i]) ? seq1[i] : '-');
            buffer.append(std::to_string(i + 1));
            buffer.push_back(isalpha(seq2[i]) ? seq2[i] : '-');
        }

        // Annotate the change in PTM recognition sequence
        if (toupper(seq1[i]) != toupper(seq2[i]) && is_motif[i][0] != is_motif[i][1]) {
            // If both sequences gained glycosylation
            if (((is_motif[i][0] ^ is_motif[i][1]) & is_motif[i][0]).any() &&
                ((is_motif[i][0] ^ is_motif[i][1]) & is_motif[i][1]).any()) {
                buffer.append("(CHO+/-)");
            }
            // If seq2 gained glycosylation
            else if (((is_motif[i][0] ^ is_motif[i][1]) & is_motif[i][1]).any()) {
                buffer.append("(CHO+)");
            }
            // If seq1 lost glycosylation
            else if (((is_motif[i][0] ^ is_motif[i][1]) & is_motif[i][0]).any()) {
                buffer.append("(CHO-)");
            }
        }
    }

    return StringVal::CopyFrom(context, (const uint8_t *)buffer.c_str(), buffer.size());
}

// Create a mutation list from two aligned strings
// Ignore resolvable ambiguations
// NT_distance()
IMPALA_UDF_EXPORT
StringVal Mutation_List_No_Ambiguous(
    FunctionContext *context, const StringVal &sequence1, const StringVal &sequence2
) {
    if (sequence1.is_null || sequence2.is_null || sequence1.len == 0 || sequence2.len == 0) {
        return StringVal::null();
    }

    std::size_t length = sequence1.len;
    if (sequence2.len < sequence1.len) {
        length = sequence2.len;
    }

    const unsigned char *seq1 = sequence1.ptr;
    const unsigned char *seq2 = sequence2.ptr;
    std::string buffer        = "";

    for (std::size_t i = 0; i < length; i++) {
        if (NTD[seq1[i]][seq2[i]]) {
            buffer += ", ";
            buffer += to_const_upper(seq1[i]);
            append_int(buffer, (i + 1));
            buffer += to_const_upper(seq2[i]);
        }
    }

    if (buffer.length() > 2) {
        buffer.erase(0, 2);
    }

    return to_StringVal(context, buffer);
}

IMPALA_UDF_EXPORT
IntVal Hamming_Distance_Pairwise_Delete(
    FunctionContext *context, const StringVal &sequence1, const StringVal &sequence2,
    const StringVal &pairwise_delete_set
) {
    if (sequence1.is_null || sequence2.is_null || pairwise_delete_set.is_null) {
        return IntVal::null();
    }
    if (sequence1.len == 0 || sequence2.len == 0) {
        return IntVal::null();
    };

    std::size_t length = sequence1.len;
    if (sequence2.len < sequence1.len) {
        length = sequence2.len;
    }

    std::string seq1((const char *)sequence1.ptr, sequence1.len);
    std::string seq2((const char *)sequence2.ptr, sequence2.len);
    std::unordered_map<char, int> m;

    if (pairwise_delete_set.len > 0) {
        std::string dset((const char *)pairwise_delete_set.ptr, pairwise_delete_set.len);
        for (std::size_t i = 0; i < pairwise_delete_set.len; i++) {
            m[dset[i]] = 1;
        }
    }

    int hamming_distance = 0;
    for (std::size_t i = 0; i < length; i++) {
        if (seq1[i] != seq2[i]) {
            seq1[i] = toupper(seq1[i]);
            seq2[i] = toupper(seq2[i]);
            if (seq1[i] != seq2[i]) {
                if (!m.contains(seq1[i]) && !m.contains(seq2[i])) {
                    hamming_distance++;
                }
            }
        }
    }

    return IntVal(hamming_distance);
}

IMPALA_UDF_EXPORT
IntVal Hamming_Distance(
    FunctionContext *context, const StringVal &sequence1, const StringVal &sequence2
) {
    if (sequence1.is_null || sequence2.is_null) {
        return IntVal::null();
    }
    if (sequence1.len == 0 || sequence2.len == 0) {
        return IntVal::null();
    };

    std::size_t length = sequence1.len;
    if (sequence2.len < sequence1.len) {
        length = sequence2.len;
    }

    std::string seq1((const char *)sequence1.ptr, sequence1.len);
    std::string seq2((const char *)sequence2.ptr, sequence2.len);

    int hamming_distance = 0;
    for (std::size_t i = 0; i < length; i++) {
        if (seq1[i] != seq2[i]) {
            seq1[i] = toupper(seq1[i]);
            seq2[i] = toupper(seq2[i]);
            if (seq1[i] != seq2[i]) {
                if (seq1[i] != '.' && seq2[i] != '.') {
                    hamming_distance++;
                }
            }
        }
    }

    return IntVal(hamming_distance);
}

IMPALA_UDF_EXPORT
IntVal Nt_Distance(
    FunctionContext *context, const StringVal &sequence1, const StringVal &sequence2
) {
    if (sequence1.is_null || sequence2.is_null || sequence1.len == 0 || sequence2.len == 0) {
        return IntVal::null();
    }

    std::size_t length = sequence1.len;
    if (sequence2.len < sequence1.len) {
        length = sequence2.len;
    }

    const uint8_t *seq1 = sequence1.ptr;
    const uint8_t *seq2 = sequence2.ptr;

    int nt_distance = 0;
    for (std::size_t i = 0; i < length; i++) {
        nt_distance += NTD[seq1[i]][seq2[i]];
    }

    return IntVal(nt_distance);
}


/* Sequence comparison functions */
IMPALA_UDF_EXPORT
StringVal Sequence_Diff(FunctionContext *context, const StringVal &seq1, const StringVal &seq2) {

    const StringVal null_stringval = StringVal::null();

    // Check if either StringVal is NULL
    if (seq1 == null_stringval || seq2 == null_stringval)
        return StringVal::null();

    // Check if either StringVal is empty.
    if (seq1.len == 0 || seq2.len == 0)
        return StringVal::null();

    // Declare variables
    std::string_view seq_ref((const char *)seq1.ptr, seq1.len);
    std::string_view seq((const char *)seq2.ptr, seq2.len);
    std::string_view::iterator it_ref, it;
    std::string diff_seq; // will store difference from ref

    // Iterate through both strings
    it_ref = seq_ref.begin();
    it     = seq.begin();

    while (it_ref != seq_ref.end() && it != seq.end()) {

        // Compare characters
        if (toupper(*it_ref) == toupper(*it))
            diff_seq.push_back('.');
        else
            diff_seq.push_back(toupper(*it));
        it_ref++;
        it++;
    }
    return StringVal::CopyFrom(context, (const uint8_t *)diff_seq.c_str(), diff_seq.size());
}

IMPALA_UDF_EXPORT
StringVal Sequence_Diff_NT(FunctionContext *context, const StringVal &seq1, const StringVal &seq2) {

    const StringVal null_stringval = StringVal::null();

    // Check if either StringVal is NULL
    if (seq1 == null_stringval || seq2 == null_stringval)
        return StringVal::null();

    // Check if either StringVal is empty.
    if (seq1.len == 0 || seq2.len == 0)
        return StringVal::null();

    // Declare variables
    std::string_view seq_ref((const char *)seq1.ptr, seq1.len);
    std::string_view seq((const char *)seq2.ptr, seq2.len);
    std::string_view::iterator it_ref, it;
    std::string diff_seq; // will store difference from ref

    // Iterate through both strings
    it_ref = seq_ref.begin();
    it     = seq.begin();

    while (it_ref != seq_ref.end() && it != seq.end()) {
        diff_seq.push_back(NT_DIFF[*it_ref][*it]);
        it_ref++;
        it++;
    }
    return StringVal::CopyFrom(context, (const uint8_t *)diff_seq.c_str(), diff_seq.size());
}

IMPALA_UDF_EXPORT
DoubleVal Physiochemical_Distance(
    FunctionContext *context, const StringVal &sequence1, const StringVal &sequence2
) {

    if (sequence1.is_null || sequence2.is_null || sequence1.len == 0 || sequence2.len == 0) {
        return DoubleVal::null();
    }

    std::size_t length = sequence1.len;
    if (sequence2.len < sequence1.len) {
        length = sequence2.len;
    }

    const uint8_t *seq1 = sequence1.ptr;
    const uint8_t *seq2 = sequence2.ptr;

    double pcd_distance       = 0;
    unsigned int number_valid = 0;
    uint16_t buff             = 0;
    for (std::size_t i = 0; i < length; i++) {
        buff = ((uint16_t)seq1[i] << 8) | ((uint16_t)seq2[i]);
        if (PCD[buff].valid) {
            pcd_distance += PCD[buff].value;
            number_valid++;
        }
    }

    if (number_valid > 0) {
        pcd_distance /= (double)number_valid;
        return DoubleVal(pcd_distance);
    } else {
        return DoubleVal::null();
    }
}

IMPALA_UDF_EXPORT
StringVal Physiochemical_Distance_List(
    FunctionContext *context, const StringVal &sequence1, const StringVal &sequence2
) {
    if (sequence1.is_null || sequence2.is_null || sequence1.len == 0 || sequence2.len == 0) {
        return StringVal::null();
    }

    std::size_t length = sequence1.len;
    if (sequence2.len < sequence1.len) {
        length = sequence2.len;
    }

    const uint8_t *seq1 = sequence1.ptr;
    const uint8_t *seq2 = sequence2.ptr;

    std::string result = "";

    std::size_t i = 0;
    uint16_t key  = 0;
    key           = ((uint16_t)seq1[i] << 8) | ((uint16_t)seq2[i]);
    if (PCD[key].valid) {
        result += std::to_string(PCD[key].value);
    } else {
        result += "NA";
    }

    for (i = 1; i < length; i++) {
        key = ((uint16_t)seq1[i] << 8) | ((uint16_t)seq2[i]);
        if (PCD[key].valid) {
            result += " " + std::to_string(PCD[key].value);
        } else {
            result += " NA";
        }
    }

    return to_StringVal(context, result);
}

IMPALA_UDF_EXPORT
BooleanVal Contains_An_Element(
    FunctionContext *context, const StringVal &mystring, const StringVal &list_of_items,
    const StringVal &delimVal
) {
    if (mystring.is_null || list_of_items.is_null || delimVal.is_null) {
        return BooleanVal::null();
    }
    if (mystring.len == 0 || list_of_items.len == 0) {
        return BooleanVal(false);
    } else if (delimVal.len == 0) {
        std::string haystack((const char *)mystring.ptr, mystring.len);
        std::string needles((const char *)list_of_items.ptr, list_of_items.len);
        return BooleanVal(haystack.find_first_of(needles) != std::string::npos);
    }

    std::string s1((const char *)mystring.ptr, mystring.len);
    std::string s2((const char *)list_of_items.ptr, list_of_items.len);
    std::string delim((const char *)delimVal.ptr, delimVal.len);
    std::vector<std::string> tokens = split_by_substr(s2, delim);

    // if the delim = string, then of course nothing can be found
    if (tokens.size() == 0) {
        return BooleanVal(false);
    }

    // otherwise search for the element
    for (std::vector<std::string>::const_iterator i = tokens.begin(); i < tokens.end(); ++i) {
        if (s1.find(*i) != std::string::npos && (*i).length() > 0) {
            return BooleanVal(true);
        }
    }

    // otherwise element was never found
    return BooleanVal(false);
}

IMPALA_UDF_EXPORT
BooleanVal Is_An_Element(
    FunctionContext *context, const StringVal &needle, const StringVal &list_of_items,
    const StringVal &delimVal
) {
    if (needle.is_null || list_of_items.is_null || delimVal.is_null) {
        return BooleanVal::null();
    }
    if (needle.len == 0 || list_of_items.len == 0) {
        return BooleanVal(false);
    } else if (delimVal.len == 0) {
        return character_in_string(needle, list_of_items);
    }

    std::string s1((const char *)needle.ptr, needle.len);
    std::string s2((const char *)list_of_items.ptr, list_of_items.len);
    std::string delim((const char *)delimVal.ptr, delimVal.len);
    std::vector<std::string> tokens = split_by_substr(s2, delim);

    if (tokens.size() == 0) {
        return BooleanVal(false);
    }
    for (std::vector<std::string>::const_iterator i = tokens.begin(); i < tokens.end(); ++i) {
        if (s1 == (*i) && (*i).length() > 0) {
            return BooleanVal(true);
        }
    }
    return BooleanVal(false);
}

IMPALA_UDF_EXPORT
BooleanVal Contains_Symmetric(
    FunctionContext *context, const StringVal &string1, const StringVal &string2
) {
    if (string1.is_null || string2.is_null) {
        return BooleanVal::null();
    }
    if ((string1.len == 0) != (string2.len == 0)) {
        return BooleanVal(false);
    }

    std::string s1((const char *)string1.ptr, string1.len);
    std::string s2((const char *)string2.ptr, string2.len);

    if (s1.find(s2) != std::string::npos || s2.find(s1) != std::string::npos) {
        return BooleanVal(true);
    } else {
        return BooleanVal(false);
    }
}

IMPALA_UDF_EXPORT
StringVal nt_id(FunctionContext *context, const StringVal &sequence) {
    if (sequence.is_null || sequence.len == 0) {
        return StringVal::null();
    }
    std::string seq((const char *)sequence.ptr, sequence.len);
    boost::remove_erase_if(seq, boost::is_any_of("\n\r\t :.~-"));
    boost::to_upper(seq);

    unsigned char obuf[21];
    SHA1((const unsigned char *)seq.c_str(), seq.size(), obuf);

    char buffer[42 * sizeof(char)];
    int j;
    for (j = 0; j < 20; j++) {
        sprintf(&buffer[2 * j * sizeof(char)], "%02x", obuf[j]);
    }

    return to_StringVal(context, buffer);
}

IMPALA_UDF_EXPORT
StringVal nt_std(FunctionContext *context, const StringVal &sequence) {
    if (sequence.is_null || sequence.len == 0) {
        return StringVal::null();
    }
    std::string seq((const char *)sequence.ptr, sequence.len);
    boost::remove_erase_if(seq, boost::is_any_of("\n\r\t :.~-"));
    boost::to_upper(seq);

    return to_StringVal(context, seq);
}

IMPALA_UDF_EXPORT
StringVal aa_std(FunctionContext *context, const StringVal &sequence) {
    if (sequence.is_null || sequence.len == 0) {
        return StringVal::null();
    }
    std::string seq((const char *)sequence.ptr, sequence.len);
    boost::remove_erase_if(seq, boost::is_any_of("\n\r\t :.-"));
    boost::to_upper(seq);

    return to_StringVal(context, seq);
}

IMPALA_UDF_EXPORT
StringVal variant_hash(FunctionContext *context, const StringVal &sequence) {
    if (sequence.is_null || sequence.len == 0) {
        return StringVal::null();
    }
    std::string seq((const char *)sequence.ptr, sequence.len);
    boost::remove_erase_if(seq, boost::is_any_of("\n\r\t :.-"));
    boost::to_upper(seq);

    unsigned char obuf[17];
    MD5((const unsigned char *)seq.c_str(), seq.size(), obuf);

    char buffer[34 * sizeof(char)];
    int j;
    for (j = 0; j < 16; j++) {
        sprintf(&buffer[2 * j * sizeof(char)], "%02x", obuf[j]);
    }

    return to_StringVal(context, buffer);
}

IMPALA_UDF_EXPORT
StringVal md5(FunctionContext *context, int num_vars, const StringVal *args) {
    if (num_vars == 0 || args[0].is_null) {
        return StringVal::null();
    }

    std::string input((const char *)args[0].ptr, args[0].len);
    const char delim = '\a';
    for (int i = 1; i < num_vars; i++) {
        if (args[i].is_null) {
            return StringVal::null();
        } else if (args[i].len == 0) {
            input += delim;
        } else {
            std::string_view next_var((const char *)args[i].ptr, args[i].len);
            input += delim;
            input += next_var;
        }
    }
    if (input.size() == 0) {
        return StringVal::null();
    }

    unsigned char obuf[17];
    MD5((const unsigned char *)input.c_str(), input.size(), obuf);

    StringVal hash(context, 32);
    // Courtesy:
    // https://stackoverflow.com/questions/6357031/how-do-you-convert-a-byte-array-to-a-hexadecimal-string-in-c/17147874#17147874
    constexpr unsigned char HEX[17] = "0123456789abcdef";
    auto *p                         = hash.ptr;
    for (int j = 0; j < 16; j++) {
        hash.ptr[2 * j]     = HEX[(obuf[j] >> 4) & 0x0F];
        hash.ptr[2 * j + 1] = HEX[(obuf[j]) & 0x0F];
    }

    return hash;
}

IMPALA_UDF_EXPORT
IntVal Number_Deletions(FunctionContext *context, const StringVal &sequence) {
    if (sequence.is_null) {
        return IntVal::null();
    }
    if (sequence.len == 0) {
        return IntVal(0);
    }

    std::string seq((const char *)sequence.ptr, sequence.len);
    int number_of_indels = 0;
    int open             = 0;

    for (int i = 1; i < seq.size(); i++) {
        if (seq[i] == '-') {
            if (isalpha(seq[i - 1])) {
                open = 1;
            }
        } else if (isalpha(seq[i])) {
            if (open > 0) {
                number_of_indels++;
                open = 0;
            }
        }
    }

    return IntVal(number_of_indels);
}

IMPALA_UDF_EXPORT
IntVal Longest_Deletion(FunctionContext *context, const StringVal &sequence) {
    if (sequence.is_null) {
        return IntVal::null();
    }
    if (sequence.len == 0) {
        return IntVal(0);
    }

    std::string seq((const char *)sequence.ptr, sequence.len);
    int longest_del = 0; // Longest deletion length
    int open        = 0; // Open deletion length

    for (int i = 1; i < seq.size(); i++) {
        if (seq[i] == '-') {
            if (isalpha(seq[i - 1])) {
                open = 1;
            } else if (open > 0) {
                open++;
            }
        } else if (isalpha(seq[i])) {
            if (open > 0) {
                if (open > longest_del) {
                    longest_del = open;
                }
                open = 0;
            }
        }
    }

    return IntVal(longest_del);
}
