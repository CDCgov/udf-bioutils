// Samuel S. Shepard, CDC
// Impala user-defined functions for CDC biofinformatics.
// Relies on Cloudera headers being installed.
// Current version supports C++20

#include <algorithm>
#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/range/algorithm_ext/erase.hpp>
#include <cctype>
#include <cmath>
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

#include "common.h"
#include "udf-bioutils.h"
#include "udx-inlines.h"

struct LookupEntry {
    bool valid   = false;
    double value = 0.0;
};

// courtesy:
// https://www.codegrepper.com/code-examples/cpp/round+double+to+n+decimal+places+c%2B%2B
constexpr double roundoff(double value, unsigned int prec) {
    double pow_10 = pow(10.0f, (float)prec);
    return std::round(value * pow_10) / pow_10;
}

constexpr auto init_pcd() {
    // physio-chemical factors
    //  Atchley et al. 2008
    //  "Solving the protein sequence metric problem."
    //  Proc Natl Acad Sci U S A. 2005 May 3;102(18):6395-400. Epub 2005 Apr 25.
    //  NOTE: Old PCD did not include X as valid
    const char aa[41]       = {'A', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'K', 'L', 'M', 'N', 'P', 'Q',
                               'R', 'S', 'T', 'V', 'W', 'Y', 'a', 'c', 'd', 'e', 'f', 'g', 'h', 'i',
                               'k', 'l', 'm', 'n', 'p', 'q', 'r', 's', 't', 'v', 'w', 'y', '-'};
    const double pcf[41][5] = {
        {-0.59, -1.3, -0.73, 1.57, -0.15},
        {-1.34, 0.47, -0.86, -1.02, -0.26},
        {1.05, 0.3, -3.66, -0.26, -3.24},
        {1.36, -1.45, 1.48, 0.11, -0.84},
        {-1.01, -0.59, 1.89, -0.4, 0.41},
        {-0.38, 1.65, 1.33, 1.05, 2.06},
        {0.34, -0.42, -1.67, -1.47, -0.08},
        {-1.24, -0.55, 2.13, 0.39, 0.82},
        {1.83, -0.56, 0.53, -0.28, 1.65},
        {-1.02, -0.99, -1.51, 1.27, -0.91},
        {-0.66, -1.52, 2.22, -1.01, 1.21},
        {0.95, 0.83, 1.3, -0.17, 0.93},
        {0.19, 2.08, -1.63, 0.42, -1.39},
        {0.93, -0.18, -3.01, -0.5, -1.85},
        {1.54, -0.06, 1.5, 0.44, 2.9},
        {-0.23, 1.4, -4.76, 0.67, -2.65},
        {-0.03, 0.33, 2.21, 0.91, 1.31},
        {-1.34, -0.28, -0.54, 1.24, -1.26},
        {-0.6, 0.01, 0.67, -2.13, -0.18},
        {0.26, 0.83, 3.1, -0.84, 1.51},
        {-0.59, -1.3, -0.73, 1.57, -0.15},
        {-1.34, 0.47, -0.86, -1.02, -0.26},
        {1.05, 0.3, -3.66, -0.26, -3.24},
        {1.36, -1.45, 1.48, 0.11, -0.84},
        {-1.01, -0.59, 1.89, -0.4, 0.41},
        {-0.38, 1.65, 1.33, 1.05, 2.06},
        {0.34, -0.42, -1.67, -1.47, -0.08},
        {-1.24, -0.55, 2.13, 0.39, 0.82},
        {1.83, -0.56, 0.53, -0.28, 1.65},
        {-1.02, -0.99, -1.51, 1.27, -0.91},
        {-0.66, -1.52, 2.22, -1.01, 1.21},
        {0.95, 0.83, 1.3, -0.17, 0.93},
        {0.19, 2.08, -1.63, 0.42, -1.39},
        {0.93, -0.18, -3.01, -0.5, -1.85},
        {1.54, -0.06, 1.5, 0.44, 2.9},
        {-0.23, 1.4, -4.76, 0.67, -2.65},
        {-0.03, 0.33, 2.21, 0.91, 1.31},
        {-1.34, -0.28, -0.54, 1.24, -1.26},
        {-0.6, 0.01, 0.67, -2.13, -0.18},
        {0.26, 0.83, 3.1, -0.84, 1.51},
        {0, 0, 0, 0, 0}
    };
    // Old PCD did not count X as valid and used 6 fixed decimal places
    std::array<LookupEntry, 65536> pcd{};
    for (int aa1 = 0; aa1 < 41; aa1++) {
        for (int aa2 = 0; aa2 < 41; aa2++) {
            uint16_t pair   = ((uint16_t)aa[aa1] << 8) | ((uint16_t)aa[aa2]);
            double distance = 0;
            for (int k = 0; k < 5; k++) {
                distance += pow(pcf[aa1][k] - pcf[aa2][k], 2);
            }
            pcd[pair].valid = true;
            pcd[pair].value = roundoff(sqrt(distance), 6);
        }
    }
    return pcd;
}
constexpr auto PCD = init_pcd();

std::unordered_map<std::string, std::string> gc = {
    {"TAA", "*"}, {"TAG", "*"}, {"TAR", "*"}, {"TGA", "*"}, {"TRA", "*"}, {"GCA", "A"},
    {"GCB", "A"}, {"GCC", "A"}, {"GCD", "A"}, {"GCG", "A"}, {"GCH", "A"}, {"GCK", "A"},
    {"GCM", "A"}, {"GCN", "A"}, {"GCR", "A"}, {"GCS", "A"}, {"GCT", "A"}, {"GCV", "A"},
    {"GCW", "A"}, {"GCY", "A"}, {"TGC", "C"}, {"TGT", "C"}, {"TGY", "C"}, {"GAC", "D"},
    {"GAT", "D"}, {"GAY", "D"}, {"GAA", "E"}, {"GAG", "E"}, {"GAR", "E"}, {"TTC", "F"},
    {"TTT", "F"}, {"TTY", "F"}, {"GGA", "G"}, {"GGB", "G"}, {"GGC", "G"}, {"GGD", "G"},
    {"GGG", "G"}, {"GGH", "G"}, {"GGK", "G"}, {"GGM", "G"}, {"GGN", "G"}, {"GGR", "G"},
    {"GGS", "G"}, {"GGT", "G"}, {"GGV", "G"}, {"GGW", "G"}, {"GGY", "G"}, {"CAC", "H"},
    {"CAT", "H"}, {"CAY", "H"}, {"ATA", "I"}, {"ATC", "I"}, {"ATH", "I"}, {"ATM", "I"},
    {"ATT", "I"}, {"ATW", "I"}, {"ATY", "I"}, {"AAA", "K"}, {"AAG", "K"}, {"AAR", "K"},
    {"CTA", "L"}, {"CTB", "L"}, {"CTC", "L"}, {"CTD", "L"}, {"CTG", "L"}, {"CTH", "L"},
    {"CTK", "L"}, {"CTM", "L"}, {"CTN", "L"}, {"CTR", "L"}, {"CTS", "L"}, {"CTT", "L"},
    {"CTV", "L"}, {"CTW", "L"}, {"CTY", "L"}, {"TTA", "L"}, {"TTG", "L"}, {"TTR", "L"},
    {"YTA", "L"}, {"YTG", "L"}, {"YTR", "L"}, {"ATG", "M"}, {"AAC", "N"}, {"AAT", "N"},
    {"AAY", "N"}, {"CCA", "P"}, {"CCB", "P"}, {"CCC", "P"}, {"CCD", "P"}, {"CCG", "P"},
    {"CCH", "P"}, {"CCK", "P"}, {"CCM", "P"}, {"CCN", "P"}, {"CCR", "P"}, {"CCS", "P"},
    {"CCT", "P"}, {"CCV", "P"}, {"CCW", "P"}, {"CCY", "P"}, {"CAA", "Q"}, {"CAG", "Q"},
    {"CAR", "Q"}, {"AGA", "R"}, {"AGG", "R"}, {"AGR", "R"}, {"CGA", "R"}, {"CGB", "R"},
    {"CGC", "R"}, {"CGD", "R"}, {"CGG", "R"}, {"CGH", "R"}, {"CGK", "R"}, {"CGM", "R"},
    {"CGN", "R"}, {"CGR", "R"}, {"CGS", "R"}, {"CGT", "R"}, {"CGV", "R"}, {"CGW", "R"},
    {"CGY", "R"}, {"MGA", "R"}, {"MGG", "R"}, {"MGR", "R"}, {"AGC", "S"}, {"AGT", "S"},
    {"AGY", "S"}, {"TCA", "S"}, {"TCB", "S"}, {"TCC", "S"}, {"TCD", "S"}, {"TCG", "S"},
    {"TCH", "S"}, {"TCK", "S"}, {"TCM", "S"}, {"TCN", "S"}, {"TCR", "S"}, {"TCS", "S"},
    {"TCT", "S"}, {"TCV", "S"}, {"TCW", "S"}, {"TCY", "S"}, {"ACA", "T"}, {"ACB", "T"},
    {"ACC", "T"}, {"ACD", "T"}, {"ACG", "T"}, {"ACH", "T"}, {"ACK", "T"}, {"ACM", "T"},
    {"ACN", "T"}, {"ACR", "T"}, {"ACS", "T"}, {"ACT", "T"}, {"ACV", "T"}, {"ACW", "T"},
    {"ACY", "T"}, {"GTA", "V"}, {"GTB", "V"}, {"GTC", "V"}, {"GTD", "V"}, {"GTG", "V"},
    {"GTH", "V"}, {"GTK", "V"}, {"GTM", "V"}, {"GTN", "V"}, {"GTR", "V"}, {"GTS", "V"},
    {"GTT", "V"}, {"GTV", "V"}, {"GTW", "V"}, {"GTY", "V"}, {"TGG", "W"}, {"TAC", "Y"},
    {"TAT", "Y"}, {"TAY", "Y"}, {"---", "-"}, {"...", "."}, {"~~~", "~"}
};

// courtesy SN
constexpr auto to_const_upper(char c) { return (c >= 'a' && c <= 'z' ? (c - 'a') + 'A' : c); }

// Nucleotide distance matrix
constexpr auto init_ntd() {
    std::array<std::array<int, 256>, 256> ntd = {0};

    // Comparable nucleotide codes
    // Alpha includes null byte
    const std::size_t A     = 33;
    const char alpha[A + 1] = "acgturyswkmbdhvn-ACGTURYSWKMBDHVN";

    // Nuclotides that are resolvable as equal
    // See: http://www.bioinformatics.org/sms/iupac.html
    const std::size_t E           = 57;
    const char equal_base1[E + 1] = "NNNNNNNNNNNNNNNBBBBBBBDDDDDDDHHHHHHHVVVVVVRRYYYSSWWWKKKMM";
    const char equal_base2[E + 1] = "ACGTURYSWKMBDHVCGTUYSKAGTURWKACTUYWMACGRSMAGCTUCGATUGTUAC";
    uint16_t equal_bases[E]       = {0};

    for (int k = 0; k < E; k++) {
        uint16_t index = ((uint16_t)equal_base1[k] << 8) | ((uint16_t)equal_base2[k]);
        equal_bases[k] = index;
    }

    for (int i = 0; i < A; i++) {
        for (int j = 0; j < A; j++) {
            char b1 = to_const_upper(alpha[i]);
            char b2 = to_const_upper(alpha[j]);
            if (b1 != b2) {
                bool ambig_equal_not_found = true;
                uint16_t key               = ((uint16_t)b1 << 8) | ((uint16_t)b2);
                uint16_t rev_key           = ((uint16_t)b2 << 8) | ((uint16_t)b1);

                for (int k = 0; k < E; k++) {
                    if (equal_bases[k] == key || equal_bases[k] == rev_key) {
                        ambig_equal_not_found = false;
                        break;
                    }
                }

                if (ambig_equal_not_found) {
                    ntd[alpha[i]][alpha[j]] = 1;
                }
            }
        }
    }
    return ntd;
}
// Nucleotide Distance Matrix
constexpr auto NTD = init_ntd();

constexpr auto init_rcm() {
    std::array<char, 256> rcm = {0};
    for (int i = 0; i < 256; i++) {
        rcm[i] = i;
    }

    // Reverse complement
    // Note: rc(WSNwsn-.) = WSNwsn-.
    const std::size_t R  = 26;
    const char fs[R + 1] = "gcatrykmbvdhuGCATRYKMBVDHU";
    const char rs[R + 1] = "cgtayrmkvbhdaCGTAYRMKVBHDA";

    for (int k = 0; k < R; k++) {
        rcm[fs[k]] = rs[k];
    }

    return rcm;
}
// Reverse Complement Matrix
constexpr auto RCM = init_rcm();


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
    std::locale loc;

    // Initialize positions
    long unsigned int N           = bases.length();
    long unsigned int R           = N % 3;
    long unsigned int codon_index = 0;

    for (codon_index = 0; codon_index + 2 < N; codon_index += 3) {
        // get codon and ignore case
        codon = bases.substr(codon_index, 3);
        for (std::string::size_type i = 0; i < 3; ++i) {
            codon[i] = std::toupper(codon[i], loc);
        }

        if (gc.count(codon) > 0) {
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
    } catch (const boost::exception &e) {
        return IntVal::null();
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
        } else if (tokens.size() == 2) {
            year  = std::stoi(tokens[0]);
            month = std::stoi(tokens[1]);
            day   = 1;
        } else if (tokens.size() == 1) {
            year  = std::stoi(tokens[0]);
            month = 1;
            day   = 1;
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
    } catch (const boost::exception &e) {
        return IntVal::null();
    } catch (std::invalid_argument &e) {
        return IntVal::null();
    } catch (std::out_of_range &e) {
        return IntVal::null();
    } catch (...) {
        return IntVal::null();
    }
}

IMPALA_UDF_EXPORT
StringVal Substring_By_Range(
    FunctionContext *context, const StringVal &sequence, const StringVal &rangeMap
) {
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
    for (std::size_t i = 0; i < length; i++) {
        if (seq1[i] != seq2[i]) {
            seq1[i] = toupper(seq1[i]);
            seq2[i] = toupper(seq2[i]);
            if (seq1[i] != seq2[i]) {
                if (seq1[i] != '.' && seq2[i] != '.') {
                    if (buffer.length() > 0) {
                        buffer += ", ";
                        buffer += seq1[i];
                        buffer += boost::lexical_cast<std::string>(i + 1);
                        buffer += seq2[i];
                    } else {
                        buffer = seq1[i] + boost::lexical_cast<std::string>(i + 1) + seq2[i];
                    }

                    // GLYCOSYLATION ADD
                    add_gly = 0;

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
                    loss_gly = 0;

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

                    if (add_gly) {
                        buffer += "-ADD";
                    }
                    if (loss_gly) {
                        buffer += "-LOSS";
                    }
                    if (add_gly || loss_gly) {
                        buffer += "-GLY";
                    }
                }
            }
        }
    }

    return to_StringVal(context, buffer);
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
                if (m.count(seq1[i]) == 0 && m.count(seq2[i]) == 0) {
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
