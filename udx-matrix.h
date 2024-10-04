#include <array>
#include <cmath>
#include <cstdint>
#include <unordered_map>
using namespace std;


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
    const int A             = 33;
    const char alpha[A + 1] = "acgturyswkmbdhvn-ACGTURYSWKMBDHVN";

    // Nuclotides that are resolvable as equal
    // See: http://www.bioinformatics.org/sms/iupac.html
    const int E                   = 57;
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

// Nucleotide distance matrix
constexpr auto init_nt_diff() {
    std::array<std::array<char, 256>, 256> ntd;
    for (auto &inner : ntd) {
        inner.fill('?');
    }

    // Comparable nucleotide codes
    // Alpha includes null byte
    const int A             = 33;
    const char alpha[A + 1] = "acgturyswkmbdhvn-ACGTURYSWKMBDHVN";

    // Nuclotides that are resolvable as equal
    // See: http://www.bioinformatics.org/sms/iupac.html
    const int E                   = 58;
    const char equal_base1[E + 1] = "NNNNNNNNNNNNNNNBBBBBBBDDDDDDDHHHHHHHVVVVVVRRYYYSSWWWKKKMMT";
    const char equal_base2[E + 1] = "ACGTURYSWKMBDHVCGTUYSKAGTURWKACTUYWMACGRSMAGCTUCGATUGTUACU";
    uint16_t equal_bases[E]       = {'?'};

    for (int k = 0; k < E; k++) {
        uint16_t index = ((uint16_t)equal_base1[k] << 8) | ((uint16_t)equal_base2[k]);
        equal_bases[k] = index;
    }

    for (int i = 0; i < A; i++) {
        for (int j = 0; j < A; j++) {
            char b1                 = to_const_upper(alpha[i]);
            char b2                 = to_const_upper(alpha[j]);
            ntd[alpha[i]][alpha[j]] = '.';

            if (b1 != b2) {
                uint16_t key            = ((uint16_t)b1 << 8) | ((uint16_t)b2);
                uint16_t rev_key        = ((uint16_t)b2 << 8) | ((uint16_t)b1);
                ntd[alpha[i]][alpha[j]] = alpha[j];

                for (int k = 0; k < E; k++) {
                    if (equal_bases[k] == key || equal_bases[k] == rev_key) {
                        ntd[alpha[i]][alpha[j]] = '.';
                        break;
                    }
                }
            }
        }
    }
    return ntd;
}
// Nucleotide Distance Matrix
constexpr auto NT_DIFF = init_nt_diff();

constexpr auto init_rcm() {
    std::array<char, 256> rcm = {0};
    for (int i = 0; i < 256; i++) {
        rcm[i] = i;
    }

    // Reverse complement
    // Note: rc(WSNwsn-.) = WSNwsn-.
    const int R          = 26;
    const char fs[R + 1] = "gcatrykmbvdhuGCATRYKMBVDHU";
    const char rs[R + 1] = "cgtayrmkvbhdaCGTAYRMKVBHDA";

    for (int k = 0; k < R; k++) {
        rcm[fs[k]] = rs[k];
    }

    return rcm;
}
// Reverse Complement Matrix
constexpr auto RCM = init_rcm();

constexpr std::array<unsigned char, 256> TO_DNA_PROFILE_INDEX = []() {
    const unsigned char FROM_BYTE[] = "acgtunACGTUN";
    const unsigned char THE_INDEX[] = "012334012334";

    std::array<unsigned char, 256> v{};
    v.fill(4);
    for (std::size_t i = 0; i < sizeof(FROM_BYTE) - 1; ++i) {
        v[FROM_BYTE[i]] = THE_INDEX[i] - '0';
    }
    return v;
}();

inline std::size_t toDNAProfileIndex(unsigned char b) { return TO_DNA_PROFILE_INDEX[b]; }

inline auto buildSubMatrix(string seq_1, string seq_2) {
    std::array<std::array<unsigned int, 4>, 4> sub = {0};

    for (size_t i = 0; i < min(seq_1.length(), seq_2.length()); ++i) {
        int idx1 = toDNAProfileIndex(seq_1[i]);
        int idx2 = toDNAProfileIndex(seq_2[i]);
        if (idx1 < 4 && idx2 < 4) {
            sub[idx1][idx2]++;
        }
    }
    return sub;
}