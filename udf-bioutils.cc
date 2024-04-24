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

std::unordered_map<std::string, char> gc = {
    {"TAA", '*'}, {"TAG", '*'}, {"TAR", '*'}, {"TGA", '*'}, {"TRA", '*'}, {"GCA", 'A'},
    {"GCB", 'A'}, {"GCC", 'A'}, {"GCD", 'A'}, {"GCG", 'A'}, {"GCH", 'A'}, {"GCK", 'A'},
    {"GCM", 'A'}, {"GCN", 'A'}, {"GCR", 'A'}, {"GCS", 'A'}, {"GCT", 'A'}, {"GCV", 'A'},
    {"GCW", 'A'}, {"GCY", 'A'}, {"TGC", 'C'}, {"TGT", 'C'}, {"TGY", 'C'}, {"GAC", 'D'},
    {"GAT", 'D'}, {"GAY", 'D'}, {"GAA", 'E'}, {"GAG", 'E'}, {"GAR", 'E'}, {"TTC", 'F'},
    {"TTT", 'F'}, {"TTY", 'F'}, {"GGA", 'G'}, {"GGB", 'G'}, {"GGC", 'G'}, {"GGD", 'G'},
    {"GGG", 'G'}, {"GGH", 'G'}, {"GGK", 'G'}, {"GGM", 'G'}, {"GGN", 'G'}, {"GGR", 'G'},
    {"GGS", 'G'}, {"GGT", 'G'}, {"GGV", 'G'}, {"GGW", 'G'}, {"GGY", 'G'}, {"CAC", 'H'},
    {"CAT", 'H'}, {"CAY", 'H'}, {"ATA", 'I'}, {"ATC", 'I'}, {"ATH", 'I'}, {"ATM", 'I'},
    {"ATT", 'I'}, {"ATW", 'I'}, {"ATY", 'I'}, {"AAA", 'K'}, {"AAG", 'K'}, {"AAR", 'K'},
    {"CTA", 'L'}, {"CTB", 'L'}, {"CTC", 'L'}, {"CTD", 'L'}, {"CTG", 'L'}, {"CTH", 'L'},
    {"CTK", 'L'}, {"CTM", 'L'}, {"CTN", 'L'}, {"CTR", 'L'}, {"CTS", 'L'}, {"CTT", 'L'},
    {"CTV", 'L'}, {"CTW", 'L'}, {"CTY", 'L'}, {"TTA", 'L'}, {"TTG", 'L'}, {"TTR", 'L'},
    {"YTA", 'L'}, {"YTG", 'L'}, {"YTR", 'L'}, {"ATG", 'M'}, {"AAC", 'N'}, {"AAT", 'N'},
    {"AAY", 'N'}, {"CCA", 'P'}, {"CCB", 'P'}, {"CCC", 'P'}, {"CCD", 'P'}, {"CCG", 'P'},
    {"CCH", 'P'}, {"CCK", 'P'}, {"CCM", 'P'}, {"CCN", 'P'}, {"CCR", 'P'}, {"CCS", 'P'},
    {"CCT", 'P'}, {"CCV", 'P'}, {"CCW", 'P'}, {"CCY", 'P'}, {"CAA", 'Q'}, {"CAG", 'Q'},
    {"CAR", 'Q'}, {"AGA", 'R'}, {"AGG", 'R'}, {"AGR", 'R'}, {"CGA", 'R'}, {"CGB", 'R'},
    {"CGC", 'R'}, {"CGD", 'R'}, {"CGG", 'R'}, {"CGH", 'R'}, {"CGK", 'R'}, {"CGM", 'R'},
    {"CGN", 'R'}, {"CGR", 'R'}, {"CGS", 'R'}, {"CGT", 'R'}, {"CGV", 'R'}, {"CGW", 'R'},
    {"CGY", 'R'}, {"MGA", 'R'}, {"MGG", 'R'}, {"MGR", 'R'}, {"AGC", 'S'}, {"AGT", 'S'},
    {"AGY", 'S'}, {"TCA", 'S'}, {"TCB", 'S'}, {"TCC", 'S'}, {"TCD", 'S'}, {"TCG", 'S'},
    {"TCH", 'S'}, {"TCK", 'S'}, {"TCM", 'S'}, {"TCN", 'S'}, {"TCR", 'S'}, {"TCS", 'S'},
    {"TCT", 'S'}, {"TCV", 'S'}, {"TCW", 'S'}, {"TCY", 'S'}, {"ACA", 'T'}, {"ACB", 'T'},
    {"ACC", 'T'}, {"ACD", 'T'}, {"ACG", 'T'}, {"ACH", 'T'}, {"ACK", 'T'}, {"ACM", 'T'},
    {"ACN", 'T'}, {"ACR", 'T'}, {"ACS", 'T'}, {"ACT", 'T'}, {"ACV", 'T'}, {"ACW", 'T'},
    {"ACY", 'T'}, {"GTA", 'V'}, {"GTB", 'V'}, {"GTC", 'V'}, {"GTD", 'V'}, {"GTG", 'V'},
    {"GTH", 'V'}, {"GTK", 'V'}, {"GTM", 'V'}, {"GTN", 'V'}, {"GTR", 'V'}, {"GTS", 'V'},
    {"GTT", 'V'}, {"GTV", 'V'}, {"GTW", 'V'}, {"GTY", 'V'}, {"TGG", 'W'}, {"TAC", 'Y'},
    {"TAT", 'Y'}, {"TAY", 'Y'}, {"---", '-'}, {"...", '.'}, {"~~~", '~'}
};

std::unordered_map<std::string, std::string> gc3 = {
    {"AAB", "K/N"},   {"AAD", "K/N"},   {"AAH", "K/N"},   {"AAK", "K/N"},   {"AAM", "K/N"},
    {"AAN", "K/N"},   {"AAS", "K/N"},   {"AAV", "K/N"},   {"AAW", "K/N"},   {"ABA", "I/R/T"},
    {"ABC", "I/S/T"}, {"ABG", "M/R/T"}, {"ABT", "I/S/T"}, {"ABY", "I/S/T"}, {"ADA", "I/K/R"},
    {"ADC", "I/N/S"}, {"ADG", "K/M/R"}, {"ADT", "I/N/S"}, {"ADY", "I/N/S"}, {"AGB", "R/S"},
    {"AGD", "R/S"},   {"AGH", "R/S"},   {"AGK", "R/S"},   {"AGM", "R/S"},   {"AGN", "R/S"},
    {"AGS", "R/S"},   {"AGV", "R/S"},   {"AGW", "R/S"},   {"AHA", "I/K/T"}, {"AHC", "I/N/T"},
    {"AHG", "K/M/T"}, {"AHT", "I/N/T"}, {"AHY", "I/N/T"}, {"AKA", "I/R"},   {"AKC", "I/S"},
    {"AKG", "M/R"},   {"AKH", "I/R/S"}, {"AKM", "I/R/S"}, {"AKR", "I/M/R"}, {"AKT", "I/S"},
    {"AKW", "I/R/S"}, {"AKY", "I/S"},   {"AMA", "K/T"},   {"AMB", "K/N/T"}, {"AMC", "N/T"},
    {"AMD", "K/N/T"}, {"AMG", "K/T"},   {"AMH", "K/N/T"}, {"AMK", "K/N/T"}, {"AMM", "K/N/T"},
    {"AMN", "K/N/T"}, {"AMR", "K/T"},   {"AMS", "K/N/T"}, {"AMT", "N/T"},   {"AMV", "K/N/T"},
    {"AMW", "K/N/T"}, {"AMY", "N/T"},   {"ARA", "K/R"},   {"ARC", "N/S"},   {"ARG", "K/R"},
    {"ARR", "K/R"},   {"ART", "N/S"},   {"ARY", "N/S"},   {"ASA", "R/T"},   {"ASB", "R/S/T"},
    {"ASC", "S/T"},   {"ASD", "R/S/T"}, {"ASG", "R/T"},   {"ASH", "R/S/T"}, {"ASK", "R/S/T"},
    {"ASM", "R/S/T"}, {"ASN", "R/S/T"}, {"ASR", "R/T"},   {"ASS", "R/S/T"}, {"AST", "S/T"},
    {"ASV", "R/S/T"}, {"ASW", "R/S/T"}, {"ASY", "S/T"},   {"ATB", "I/M"},   {"ATD", "I/M"},
    {"ATK", "I/M"},   {"ATN", "I/M"},   {"ATR", "I/M"},   {"ATS", "I/M"},   {"ATV", "I/M"},
    {"AVA", "K/R/T"}, {"AVC", "N/S/T"}, {"AVG", "K/R/T"}, {"AVR", "K/R/T"}, {"AVT", "N/S/T"},
    {"AVY", "N/S/T"}, {"AWA", "I/K"},   {"AWC", "I/N"},   {"AWG", "K/M"},   {"AWH", "I/K/N"},
    {"AWM", "I/K/N"}, {"AWR", "I/K/M"}, {"AWT", "I/N"},   {"AWW", "I/K/N"}, {"AWY", "I/N"},
    {"AYA", "I/T"},   {"AYB", "I/M/T"}, {"AYC", "I/T"},   {"AYD", "I/M/T"}, {"AYG", "M/T"},
    {"AYH", "I/T"},   {"AYK", "I/M/T"}, {"AYM", "I/T"},   {"AYN", "I/M/T"}, {"AYR", "I/M/T"},
    {"AYS", "I/M/T"}, {"AYT", "I/T"},   {"AYV", "I/M/T"}, {"AYW", "I/T"},   {"AYY", "I/T"},
    {"BAA", "E/Q/*"}, {"BAC", "D/H/Y"}, {"BAG", "E/Q/*"}, {"BAR", "E/Q/*"}, {"BAT", "D/H/Y"},
    {"BAY", "D/H/Y"}, {"BCA", "A/P/S"}, {"BCB", "A/P/S"}, {"BCC", "A/P/S"}, {"BCD", "A/P/S"},
    {"BCG", "A/P/S"}, {"BCH", "A/P/S"}, {"BCK", "A/P/S"}, {"BCM", "A/P/S"}, {"BCN", "A/P/S"},
    {"BCR", "A/P/S"}, {"BCS", "A/P/S"}, {"BCT", "A/P/S"}, {"BCV", "A/P/S"}, {"BCW", "A/P/S"},
    {"BCY", "A/P/S"}, {"BGA", "G/R/*"}, {"BGC", "C/G/R"}, {"BGG", "G/R/W"}, {"BGT", "C/G/R"},
    {"BGY", "C/G/R"}, {"BTA", "L/V"},   {"BTB", "F/L/V"}, {"BTC", "F/L/V"}, {"BTD", "F/L/V"},
    {"BTG", "L/V"},   {"BTH", "F/L/V"}, {"BTK", "F/L/V"}, {"BTM", "F/L/V"}, {"BTN", "F/L/V"},
    {"BTR", "L/V"},   {"BTS", "F/L/V"}, {"BTT", "F/L/V"}, {"BTV", "F/L/V"}, {"BTW", "F/L/V"},
    {"BTY", "F/L/V"}, {"CAB", "H/Q"},   {"CAD", "H/Q"},   {"CAH", "H/Q"},   {"CAK", "H/Q"},
    {"CAM", "H/Q"},   {"CAN", "H/Q"},   {"CAS", "H/Q"},   {"CAV", "H/Q"},   {"CAW", "H/Q"},
    {"CBA", "L/P/R"}, {"CBB", "L/P/R"}, {"CBC", "L/P/R"}, {"CBD", "L/P/R"}, {"CBG", "L/P/R"},
    {"CBH", "L/P/R"}, {"CBK", "L/P/R"}, {"CBM", "L/P/R"}, {"CBN", "L/P/R"}, {"CBR", "L/P/R"},
    {"CBS", "L/P/R"}, {"CBT", "L/P/R"}, {"CBV", "L/P/R"}, {"CBW", "L/P/R"}, {"CBY", "L/P/R"},
    {"CDA", "L/Q/R"}, {"CDC", "H/L/R"}, {"CDG", "L/Q/R"}, {"CDR", "L/Q/R"}, {"CDT", "H/L/R"},
    {"CDY", "H/L/R"}, {"CHA", "L/P/Q"}, {"CHC", "H/L/P"}, {"CHG", "L/P/Q"}, {"CHR", "L/P/Q"},
    {"CHT", "H/L/P"}, {"CHY", "H/L/P"}, {"CKA", "L/R"},   {"CKB", "L/R"},   {"CKC", "L/R"},
    {"CKD", "L/R"},   {"CKG", "L/R"},   {"CKH", "L/R"},   {"CKK", "L/R"},   {"CKM", "L/R"},
    {"CKN", "L/R"},   {"CKR", "L/R"},   {"CKS", "L/R"},   {"CKT", "L/R"},   {"CKV", "L/R"},
    {"CKW", "L/R"},   {"CKY", "L/R"},   {"CMA", "P/Q"},   {"CMB", "H/P/Q"}, {"CMC", "H/P"},
    {"CMD", "H/P/Q"}, {"CMG", "P/Q"},   {"CMH", "H/P/Q"}, {"CMK", "H/P/Q"}, {"CMM", "H/P/Q"},
    {"CMN", "H/P/Q"}, {"CMR", "P/Q"},   {"CMS", "H/P/Q"}, {"CMT", "H/P"},   {"CMV", "H/P/Q"},
    {"CMW", "H/P/Q"}, {"CMY", "H/P"},   {"CRA", "Q/R"},   {"CRB", "H/Q/R"}, {"CRC", "H/R"},
    {"CRD", "H/Q/R"}, {"CRG", "Q/R"},   {"CRH", "H/Q/R"}, {"CRK", "H/Q/R"}, {"CRM", "H/Q/R"},
    {"CRN", "H/Q/R"}, {"CRR", "Q/R"},   {"CRS", "H/Q/R"}, {"CRT", "H/R"},   {"CRV", "H/Q/R"},
    {"CRW", "H/Q/R"}, {"CRY", "H/R"},   {"CSA", "P/R"},   {"CSB", "P/R"},   {"CSC", "P/R"},
    {"CSD", "P/R"},   {"CSG", "P/R"},   {"CSH", "P/R"},   {"CSK", "P/R"},   {"CSM", "P/R"},
    {"CSN", "P/R"},   {"CSR", "P/R"},   {"CSS", "P/R"},   {"CST", "P/R"},   {"CSV", "P/R"},
    {"CSW", "P/R"},   {"CSY", "P/R"},   {"CVA", "P/Q/R"}, {"CVC", "H/P/R"}, {"CVG", "P/Q/R"},
    {"CVR", "P/Q/R"}, {"CVT", "H/P/R"}, {"CVY", "H/P/R"}, {"CWA", "L/Q"},   {"CWB", "H/L/Q"},
    {"CWC", "H/L"},   {"CWD", "H/L/Q"}, {"CWG", "L/Q"},   {"CWH", "H/L/Q"}, {"CWK", "H/L/Q"},
    {"CWM", "H/L/Q"}, {"CWN", "H/L/Q"}, {"CWR", "L/Q"},   {"CWS", "H/L/Q"}, {"CWT", "H/L"},
    {"CWV", "H/L/Q"}, {"CWW", "H/L/Q"}, {"CWY", "H/L"},   {"CYA", "L/P"},   {"CYB", "L/P"},
    {"CYC", "L/P"},   {"CYD", "L/P"},   {"CYG", "L/P"},   {"CYH", "L/P"},   {"CYK", "L/P"},
    {"CYM", "L/P"},   {"CYN", "L/P"},   {"CYR", "L/P"},   {"CYS", "L/P"},   {"CYT", "L/P"},
    {"CYV", "L/P"},   {"CYW", "L/P"},   {"CYY", "L/P"},   {"DAA", "E/K/*"}, {"DAC", "D/N/Y"},
    {"DAG", "E/K/*"}, {"DAR", "E/K/*"}, {"DAT", "D/N/Y"}, {"DAY", "D/N/Y"}, {"DCA", "A/S/T"},
    {"DCB", "A/S/T"}, {"DCC", "A/S/T"}, {"DCD", "A/S/T"}, {"DCG", "A/S/T"}, {"DCH", "A/S/T"},
    {"DCK", "A/S/T"}, {"DCM", "A/S/T"}, {"DCN", "A/S/T"}, {"DCR", "A/S/T"}, {"DCS", "A/S/T"},
    {"DCT", "A/S/T"}, {"DCV", "A/S/T"}, {"DCW", "A/S/T"}, {"DCY", "A/S/T"}, {"DGA", "G/R/*"},
    {"DGC", "C/G/S"}, {"DGG", "G/R/W"}, {"DGT", "C/G/S"}, {"DGY", "C/G/S"}, {"DTA", "I/L/V"},
    {"DTC", "F/I/V"}, {"DTG", "L/M/V"}, {"DTT", "F/I/V"}, {"DTY", "F/I/V"}, {"GAB", "D/E"},
    {"GAD", "D/E"},   {"GAH", "D/E"},   {"GAK", "D/E"},   {"GAM", "D/E"},   {"GAN", "D/E"},
    {"GAS", "D/E"},   {"GAV", "D/E"},   {"GAW", "D/E"},   {"GBA", "A/G/V"}, {"GBB", "A/G/V"},
    {"GBC", "A/G/V"}, {"GBD", "A/G/V"}, {"GBG", "A/G/V"}, {"GBH", "A/G/V"}, {"GBK", "A/G/V"},
    {"GBM", "A/G/V"}, {"GBN", "A/G/V"}, {"GBR", "A/G/V"}, {"GBS", "A/G/V"}, {"GBT", "A/G/V"},
    {"GBV", "A/G/V"}, {"GBW", "A/G/V"}, {"GBY", "A/G/V"}, {"GDA", "E/G/V"}, {"GDC", "D/G/V"},
    {"GDG", "E/G/V"}, {"GDR", "E/G/V"}, {"GDT", "D/G/V"}, {"GDY", "D/G/V"}, {"GHA", "A/E/V"},
    {"GHC", "A/D/V"}, {"GHG", "A/E/V"}, {"GHR", "A/E/V"}, {"GHT", "A/D/V"}, {"GHY", "A/D/V"},
    {"GKA", "G/V"},   {"GKB", "G/V"},   {"GKC", "G/V"},   {"GKD", "G/V"},   {"GKG", "G/V"},
    {"GKH", "G/V"},   {"GKK", "G/V"},   {"GKM", "G/V"},   {"GKN", "G/V"},   {"GKR", "G/V"},
    {"GKS", "G/V"},   {"GKT", "G/V"},   {"GKV", "G/V"},   {"GKW", "G/V"},   {"GKY", "G/V"},
    {"GMA", "A/E"},   {"GMB", "A/D/E"}, {"GMC", "A/D"},   {"GMD", "A/D/E"}, {"GMG", "A/E"},
    {"GMH", "A/D/E"}, {"GMK", "A/D/E"}, {"GMM", "A/D/E"}, {"GMN", "A/D/E"}, {"GMR", "A/E"},
    {"GMS", "A/D/E"}, {"GMT", "A/D"},   {"GMV", "A/D/E"}, {"GMW", "A/D/E"}, {"GMY", "A/D"},
    {"GRA", "E/G"},   {"GRB", "D/E/G"}, {"GRC", "D/G"},   {"GRD", "D/E/G"}, {"GRG", "E/G"},
    {"GRH", "D/E/G"}, {"GRK", "D/E/G"}, {"GRM", "D/E/G"}, {"GRN", "D/E/G"}, {"GRR", "E/G"},
    {"GRS", "D/E/G"}, {"GRT", "D/G"},   {"GRV", "D/E/G"}, {"GRW", "D/E/G"}, {"GRY", "D/G"},
    {"GSA", "A/G"},   {"GSB", "A/G"},   {"GSC", "A/G"},   {"GSD", "A/G"},   {"GSG", "A/G"},
    {"GSH", "A/G"},   {"GSK", "A/G"},   {"GSM", "A/G"},   {"GSN", "A/G"},   {"GSR", "A/G"},
    {"GSS", "A/G"},   {"GST", "A/G"},   {"GSV", "A/G"},   {"GSW", "A/G"},   {"GSY", "A/G"},
    {"GVA", "A/E/G"}, {"GVC", "A/D/G"}, {"GVG", "A/E/G"}, {"GVR", "A/E/G"}, {"GVT", "A/D/G"},
    {"GVY", "A/D/G"}, {"GWA", "E/V"},   {"GWB", "D/E/V"}, {"GWC", "D/V"},   {"GWD", "D/E/V"},
    {"GWG", "E/V"},   {"GWH", "D/E/V"}, {"GWK", "D/E/V"}, {"GWM", "D/E/V"}, {"GWN", "D/E/V"},
    {"GWR", "E/V"},   {"GWS", "D/E/V"}, {"GWT", "D/V"},   {"GWV", "D/E/V"}, {"GWW", "D/E/V"},
    {"GWY", "D/V"},   {"GYA", "A/V"},   {"GYB", "A/V"},   {"GYC", "A/V"},   {"GYD", "A/V"},
    {"GYG", "A/V"},   {"GYH", "A/V"},   {"GYK", "A/V"},   {"GYM", "A/V"},   {"GYN", "A/V"},
    {"GYR", "A/V"},   {"GYS", "A/V"},   {"GYT", "A/V"},   {"GYV", "A/V"},   {"GYW", "A/V"},
    {"GYY", "A/V"},   {"HAA", "K/Q/*"}, {"HAC", "H/N/Y"}, {"HAG", "K/Q/*"}, {"HAR", "K/Q/*"},
    {"HAT", "H/N/Y"}, {"HAY", "H/N/Y"}, {"HCA", "P/S/T"}, {"HCB", "P/S/T"}, {"HCC", "P/S/T"},
    {"HCD", "P/S/T"}, {"HCG", "P/S/T"}, {"HCH", "P/S/T"}, {"HCK", "P/S/T"}, {"HCM", "P/S/T"},
    {"HCN", "P/S/T"}, {"HCR", "P/S/T"}, {"HCS", "P/S/T"}, {"HCT", "P/S/T"}, {"HCV", "P/S/T"},
    {"HCW", "P/S/T"}, {"HCY", "P/S/T"}, {"HGA", "R/*"},   {"HGC", "C/R/S"}, {"HGG", "R/W"},
    {"HGR", "R/W/*"}, {"HGT", "C/R/S"}, {"HGY", "C/R/S"}, {"HTA", "I/L"},   {"HTC", "F/I/L"},
    {"HTG", "L/M"},   {"HTH", "F/I/L"}, {"HTM", "F/I/L"}, {"HTR", "I/L/M"}, {"HTT", "F/I/L"},
    {"HTW", "F/I/L"}, {"HTY", "F/I/L"}, {"KAA", "E/*"},   {"KAC", "D/Y"},   {"KAG", "E/*"},
    {"KAR", "E/*"},   {"KAT", "D/Y"},   {"KAY", "D/Y"},   {"KCA", "A/S"},   {"KCB", "A/S"},
    {"KCC", "A/S"},   {"KCD", "A/S"},   {"KCG", "A/S"},   {"KCH", "A/S"},   {"KCK", "A/S"},
    {"KCM", "A/S"},   {"KCN", "A/S"},   {"KCR", "A/S"},   {"KCS", "A/S"},   {"KCT", "A/S"},
    {"KCV", "A/S"},   {"KCW", "A/S"},   {"KCY", "A/S"},   {"KGA", "G/*"},   {"KGB", "C/G/W"},
    {"KGC", "C/G"},   {"KGG", "G/W"},   {"KGH", "C/G/*"}, {"KGK", "C/G/W"}, {"KGM", "C/G/*"},
    {"KGR", "G/W/*"}, {"KGS", "C/G/W"}, {"KGT", "C/G"},   {"KGW", "C/G/*"}, {"KGY", "C/G"},
    {"KRA", "E/G/*"}, {"KTA", "L/V"},   {"KTB", "F/L/V"}, {"KTC", "F/V"},   {"KTD", "F/L/V"},
    {"KTG", "L/V"},   {"KTH", "F/L/V"}, {"KTK", "F/L/V"}, {"KTM", "F/L/V"}, {"KTN", "F/L/V"},
    {"KTR", "L/V"},   {"KTS", "F/L/V"}, {"KTT", "F/V"},   {"KTV", "F/L/V"}, {"KTW", "F/L/V"},
    {"KTY", "F/V"},   {"MAA", "K/Q"},   {"MAC", "H/N"},   {"MAG", "K/Q"},   {"MAR", "K/Q"},
    {"MAT", "H/N"},   {"MAY", "H/N"},   {"MCA", "P/T"},   {"MCB", "P/T"},   {"MCC", "P/T"},
    {"MCD", "P/T"},   {"MCG", "P/T"},   {"MCH", "P/T"},   {"MCK", "P/T"},   {"MCM", "P/T"},
    {"MCN", "P/T"},   {"MCR", "P/T"},   {"MCS", "P/T"},   {"MCT", "P/T"},   {"MCV", "P/T"},
    {"MCW", "P/T"},   {"MCY", "P/T"},   {"MGB", "R/S"},   {"MGC", "R/S"},   {"MGD", "R/S"},
    {"MGH", "R/S"},   {"MGK", "R/S"},   {"MGM", "R/S"},   {"MGN", "R/S"},   {"MGS", "R/S"},
    {"MGT", "R/S"},   {"MGV", "R/S"},   {"MGW", "R/S"},   {"MGY", "R/S"},   {"MKA", "I/L/R"},
    {"MKG", "L/M/R"}, {"MRA", "K/Q/R"}, {"MRG", "K/Q/R"}, {"MRR", "K/Q/R"}, {"MSA", "P/R/T"},
    {"MSG", "P/R/T"}, {"MSR", "P/R/T"}, {"MTA", "I/L"},   {"MTB", "I/L/M"}, {"MTC", "I/L"},
    {"MTD", "I/L/M"}, {"MTG", "L/M"},   {"MTH", "I/L"},   {"MTK", "I/L/M"}, {"MTM", "I/L"},
    {"MTN", "I/L/M"}, {"MTR", "I/L/M"}, {"MTS", "I/L/M"}, {"MTT", "I/L"},   {"MTV", "I/L/M"},
    {"MTW", "I/L"},   {"MTY", "I/L"},   {"NGA", "G/R/*"}, {"NGG", "G/R/W"}, {"NTA", "I/L/V"},
    {"NTG", "L/M/V"}, {"RAA", "E/K"},   {"RAC", "D/N"},   {"RAG", "E/K"},   {"RAR", "E/K"},
    {"RAT", "D/N"},   {"RAY", "D/N"},   {"RCA", "A/T"},   {"RCB", "A/T"},   {"RCC", "A/T"},
    {"RCD", "A/T"},   {"RCG", "A/T"},   {"RCH", "A/T"},   {"RCK", "A/T"},   {"RCM", "A/T"},
    {"RCN", "A/T"},   {"RCR", "A/T"},   {"RCS", "A/T"},   {"RCT", "A/T"},   {"RCV", "A/T"},
    {"RCW", "A/T"},   {"RCY", "A/T"},   {"RGA", "G/R"},   {"RGB", "G/R/S"}, {"RGC", "G/S"},
    {"RGD", "G/R/S"}, {"RGG", "G/R"},   {"RGH", "G/R/S"}, {"RGK", "G/R/S"}, {"RGM", "G/R/S"},
    {"RGN", "G/R/S"}, {"RGR", "G/R"},   {"RGS", "G/R/S"}, {"RGT", "G/S"},   {"RGV", "G/R/S"},
    {"RGW", "G/R/S"}, {"RGY", "G/S"},   {"RTA", "I/V"},   {"RTB", "I/M/V"}, {"RTC", "I/V"},
    {"RTD", "I/M/V"}, {"RTG", "M/V"},   {"RTH", "I/V"},   {"RTK", "I/M/V"}, {"RTM", "I/V"},
    {"RTN", "I/M/V"}, {"RTR", "I/M/V"}, {"RTS", "I/M/V"}, {"RTT", "I/V"},   {"RTV", "I/M/V"},
    {"RTW", "I/V"},   {"RTY", "I/V"},   {"SAA", "E/Q"},   {"SAC", "D/H"},   {"SAG", "E/Q"},
    {"SAR", "E/Q"},   {"SAT", "D/H"},   {"SAY", "D/H"},   {"SCA", "A/P"},   {"SCB", "A/P"},
    {"SCC", "A/P"},   {"SCD", "A/P"},   {"SCG", "A/P"},   {"SCH", "A/P"},   {"SCK", "A/P"},
    {"SCM", "A/P"},   {"SCN", "A/P"},   {"SCR", "A/P"},   {"SCS", "A/P"},   {"SCT", "A/P"},
    {"SCV", "A/P"},   {"SCW", "A/P"},   {"SCY", "A/P"},   {"SGA", "G/R"},   {"SGB", "G/R"},
    {"SGC", "G/R"},   {"SGD", "G/R"},   {"SGG", "G/R"},   {"SGH", "G/R"},   {"SGK", "G/R"},
    {"SGM", "G/R"},   {"SGN", "G/R"},   {"SGR", "G/R"},   {"SGS", "G/R"},   {"SGT", "G/R"},
    {"SGV", "G/R"},   {"SGW", "G/R"},   {"SGY", "G/R"},   {"STA", "L/V"},   {"STB", "L/V"},
    {"STC", "L/V"},   {"STD", "L/V"},   {"STG", "L/V"},   {"STH", "L/V"},   {"STK", "L/V"},
    {"STM", "L/V"},   {"STN", "L/V"},   {"STR", "L/V"},   {"STS", "L/V"},   {"STT", "L/V"},
    {"STV", "L/V"},   {"STW", "L/V"},   {"STY", "L/V"},   {"TAB", "Y/*"},   {"TAD", "Y/*"},
    {"TAH", "Y/*"},   {"TAK", "Y/*"},   {"TAM", "Y/*"},   {"TAN", "Y/*"},   {"TAS", "Y/*"},
    {"TAV", "Y/*"},   {"TAW", "Y/*"},   {"TBA", "L/S/*"}, {"TBC", "C/F/S"}, {"TBG", "L/S/W"},
    {"TBT", "C/F/S"}, {"TBY", "C/F/S"}, {"TDA", "L/*"},   {"TDC", "C/F/Y"}, {"TDG", "L/W/*"},
    {"TDR", "L/W/*"}, {"TDT", "C/F/Y"}, {"TDY", "C/F/Y"}, {"TGB", "C/W"},   {"TGD", "C/W/*"},
    {"TGH", "C/*"},   {"TGK", "C/W"},   {"TGM", "C/*"},   {"TGN", "C/W/*"}, {"TGR", "W/*"},
    {"TGS", "C/W"},   {"TGV", "C/W/*"}, {"TGW", "C/*"},   {"THA", "L/S/*"}, {"THC", "F/S/Y"},
    {"THG", "L/S/*"}, {"THR", "L/S/*"}, {"THT", "F/S/Y"}, {"THY", "F/S/Y"}, {"TKA", "L/*"},
    {"TKC", "C/F"},   {"TKG", "L/W"},   {"TKR", "L/W/*"}, {"TKT", "C/F"},   {"TKY", "C/F"},
    {"TMA", "S/*"},   {"TMB", "S/Y/*"}, {"TMC", "S/Y"},   {"TMD", "S/Y/*"}, {"TMG", "S/*"},
    {"TMH", "S/Y/*"}, {"TMK", "S/Y/*"}, {"TMM", "S/Y/*"}, {"TMN", "S/Y/*"}, {"TMR", "S/*"},
    {"TMS", "S/Y/*"}, {"TMT", "S/Y"},   {"TMV", "S/Y/*"}, {"TMW", "S/Y/*"}, {"TMY", "S/Y"},
    {"TNA", "L/S/*"}, {"TRC", "C/Y"},   {"TRG", "W/*"},   {"TRH", "C/Y/*"}, {"TRM", "C/Y/*"},
    {"TRR", "W/*"},   {"TRT", "C/Y"},   {"TRW", "C/Y/*"}, {"TRY", "C/Y"},   {"TSA", "S/*"},
    {"TSB", "C/S/W"}, {"TSC", "C/S"},   {"TSG", "S/W"},   {"TSH", "C/S/*"}, {"TSK", "C/S/W"},
    {"TSM", "C/S/*"}, {"TSR", "S/W/*"}, {"TSS", "C/S/W"}, {"TST", "C/S"},   {"TSW", "C/S/*"},
    {"TSY", "C/S"},   {"TTB", "F/L"},   {"TTD", "F/L"},   {"TTH", "F/L"},   {"TTK", "F/L"},
    {"TTM", "F/L"},   {"TTN", "F/L"},   {"TTS", "F/L"},   {"TTV", "F/L"},   {"TTW", "F/L"},
    {"TVA", "S/*"},   {"TVC", "C/S/Y"}, {"TVG", "S/W/*"}, {"TVR", "S/W/*"}, {"TVT", "C/S/Y"},
    {"TVY", "C/S/Y"}, {"TWA", "L/*"},   {"TWC", "F/Y"},   {"TWG", "L/*"},   {"TWR", "L/*"},
    {"TWT", "F/Y"},   {"TWY", "F/Y"},   {"TYA", "L/S"},   {"TYB", "F/L/S"}, {"TYC", "F/S"},
    {"TYD", "F/L/S"}, {"TYG", "L/S"},   {"TYH", "F/L/S"}, {"TYK", "F/L/S"}, {"TYM", "F/L/S"},
    {"TYN", "F/L/S"}, {"TYR", "L/S"},   {"TYS", "F/L/S"}, {"TYT", "F/S"},   {"TYV", "F/L/S"},
    {"TYW", "F/L/S"}, {"TYY", "F/S"},   {"VAA", "E/K/Q"}, {"VAC", "D/H/N"}, {"VAG", "E/K/Q"},
    {"VAR", "E/K/Q"}, {"VAT", "D/H/N"}, {"VAY", "D/H/N"}, {"VCA", "A/P/T"}, {"VCB", "A/P/T"},
    {"VCC", "A/P/T"}, {"VCD", "A/P/T"}, {"VCG", "A/P/T"}, {"VCH", "A/P/T"}, {"VCK", "A/P/T"},
    {"VCM", "A/P/T"}, {"VCN", "A/P/T"}, {"VCR", "A/P/T"}, {"VCS", "A/P/T"}, {"VCT", "A/P/T"},
    {"VCV", "A/P/T"}, {"VCW", "A/P/T"}, {"VCY", "A/P/T"}, {"VGA", "G/R"},   {"VGB", "G/R/S"},
    {"VGC", "G/R/S"}, {"VGD", "G/R/S"}, {"VGG", "G/R"},   {"VGH", "G/R/S"}, {"VGK", "G/R/S"},
    {"VGM", "G/R/S"}, {"VGN", "G/R/S"}, {"VGR", "G/R"},   {"VGS", "G/R/S"}, {"VGT", "G/R/S"},
    {"VGV", "G/R/S"}, {"VGW", "G/R/S"}, {"VGY", "G/R/S"}, {"VTA", "I/L/V"}, {"VTC", "I/L/V"},
    {"VTG", "L/M/V"}, {"VTH", "I/L/V"}, {"VTM", "I/L/V"}, {"VTT", "I/L/V"}, {"VTW", "I/L/V"},
    {"VTY", "I/L/V"}, {"WAA", "K/*"},   {"WAC", "N/Y"},   {"WAG", "K/*"},   {"WAR", "K/*"},
    {"WAT", "N/Y"},   {"WAY", "N/Y"},   {"WCA", "S/T"},   {"WCB", "S/T"},   {"WCC", "S/T"},
    {"WCD", "S/T"},   {"WCG", "S/T"},   {"WCH", "S/T"},   {"WCK", "S/T"},   {"WCM", "S/T"},
    {"WCN", "S/T"},   {"WCR", "S/T"},   {"WCS", "S/T"},   {"WCT", "S/T"},   {"WCV", "S/T"},
    {"WCW", "S/T"},   {"WCY", "S/T"},   {"WGA", "R/*"},   {"WGC", "C/S"},   {"WGG", "R/W"},
    {"WGR", "R/W/*"}, {"WGT", "C/S"},   {"WGY", "C/S"},   {"WRA", "K/R/*"}, {"WSC", "C/S/T"},
    {"WST", "C/S/T"}, {"WSY", "C/S/T"}, {"WTA", "I/L"},   {"WTC", "F/I"},   {"WTG", "L/M"},
    {"WTH", "F/I/L"}, {"WTM", "F/I/L"}, {"WTR", "I/L/M"}, {"WTT", "F/I"},   {"WTW", "F/I/L"},
    {"WTY", "F/I"},   {"YAA", "Q/*"},   {"YAC", "H/Y"},   {"YAG", "Q/*"},   {"YAR", "Q/*"},
    {"YAT", "H/Y"},   {"YAY", "H/Y"},   {"YCA", "P/S"},   {"YCB", "P/S"},   {"YCC", "P/S"},
    {"YCD", "P/S"},   {"YCG", "P/S"},   {"YCH", "P/S"},   {"YCK", "P/S"},   {"YCM", "P/S"},
    {"YCN", "P/S"},   {"YCR", "P/S"},   {"YCS", "P/S"},   {"YCT", "P/S"},   {"YCV", "P/S"},
    {"YCW", "P/S"},   {"YCY", "P/S"},   {"YGA", "R/*"},   {"YGB", "C/R/W"}, {"YGC", "C/R"},
    {"YGG", "R/W"},   {"YGH", "C/R/*"}, {"YGK", "C/R/W"}, {"YGM", "C/R/*"}, {"YGR", "R/W/*"},
    {"YGS", "C/R/W"}, {"YGT", "C/R"},   {"YGW", "C/R/*"}, {"YGY", "C/R"},   {"YKA", "L/R/*"},
    {"YKG", "L/R/W"}, {"YRA", "Q/R/*"}, {"YTB", "F/L"},   {"YTC", "F/L"},   {"YTD", "F/L"},
    {"YTH", "F/L"},   {"YTK", "F/L"},   {"YTM", "F/L"},   {"YTN", "F/L"},   {"YTS", "F/L"},
    {"YTT", "F/L"},   {"YTV", "F/L"},   {"YTW", "F/L"},   {"YTY", "F/L"},   {"YWA", "L/Q/*"},
    {"YWG", "L/Q/*"}, {"YWR", "L/Q/*"}, {"YYA", "L/P/S"}, {"YYG", "L/P/S"}, {"YYR", "L/P/S"}
};

// courtesy SN
constexpr auto to_const_upper(char c) { return (c >= 'a' && c <= 'z' ? (c - 'a') + 'A' : c); }
constexpr auto to_const_lower(char c) { return (c >= 'A' && c <= 'Z' ? (c - 'A') + 'a' : c); }


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
