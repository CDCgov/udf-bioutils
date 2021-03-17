// udf-bioutils.cc - Sam Shepard - 2020
// Impala user-defined functions for CDC biofinformatics.
// Relies on Cloudera headers being installed.
// Current version supports C++11

#include "udf-bioutils.h"
#include "common.h"

#include <cctype>
#include <cmath>
#include <string>
#include <algorithm>
#include <vector>
#include <sstream>
#include <locale>
//#include <boost/unordered_map.hpp>
//#include <boost/unordered_set.hpp>
#include <unordered_map>
#include <unordered_set>
#include <boost/lexical_cast.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/range/algorithm_ext/erase.hpp>
#include <openssl/sha.h>
#include <openssl/md5.h>

#include "boost/date_time/gregorian/gregorian.hpp"
#include <boost/exception/all.hpp>

std::unordered_map<std::string,double> pcd = {
	{"--",0.000000},{"-A",2.249089},{"-C",1.965731},{"-D",5.015307},{"-E",2.619198},{"-F",2.295300},{"-G",3.159415},{"-H",2.290895},{"-I",2.683561},
	{"-K",2.596979},{"-L",2.596459},{"-M",3.187256},{"-N",2.043331},{"-P",3.021241},{"-Q",3.691869},{"-R",3.637142},{"-S",5.669383},{"-T",2.745560},
	{"-V",2.300174},{"-W",2.319116},{"-Y",3.654066},{"A-",2.249089},{"AA",0.000000},{"AC",3.229938},{"AD",5.170251},{"AE",3.364045},{"AF",3.426281},
	{"AG",4.259660},{"AH",3.430656},{"AI",3.390855},{"AK",3.828067},{"AL",1.247798},{"AM",4.154732},{"AN",3.901718},{"AP",3.962688},{"AQ",3.992255},
	{"AR",4.650462},{"AS",5.542608},{"AT",3.765807},{"AV",1.726268},{"AW",4.167385},{"AY",5.337790},{"C-",1.965731},{"CA",3.229938},{"CC",0.000000},
	{"CD",4.799896},{"CE",4.250329},{"CF",3.102950},{"CG",4.096022},{"CH",2.122616},{"CI",3.625603},{"CK",4.151819},{"CL",2.884978},{"CM",4.008728},
	{"CN",3.489685},{"CP",2.979329},{"CQ",3.605052},{"CR",5.124656},{"CS",5.086767},{"CT",4.165381},{"CV",2.602403},{"CW",2.082931},{"CY",4.640743},
	{"D-",5.015307},{"DA",5.170251},{"DC",4.799896},{"DD",0.000000},{"DE",5.956098},{"DF",7.012867},{"DG",7.653339},{"DH",4.053677},{"DI",7.509780},
	{"DK",6.543439},{"DL",4.282674},{"DM",7.821502},{"DN",6.503038},{"DP",3.451637},{"DQ",1.630031},{"DR",8.073717},{"DS",2.295953},{"DT",7.595762},
	{"DV",4.685435},{"DW",5.866515},{"DY",8.336756},{"E-",2.619198},{"EA",3.364045},{"EC",4.250329},{"ED",5.956098},{"EE",0.000000},{"EF",2.889152},
	{"EG",4.685478},{"EH",3.885589},{"EI",3.290365},{"EK",2.875361},{"EL",4.020771},{"EM",3.176445},{"EN",2.934314},{"EP",4.888814},{"EQ",4.832194},
	{"ER",4.007668},{"ES",7.292318},{"ET",3.300894},{"EV",3.767307},{"EW",3.475989},{"EY",3.931641},{"F-",2.295300},{"FA",3.426281},{"FC",3.102950},
	{"FD",7.012867},{"FE",2.889152},{"FF",0.000000},{"FG",3.248554},{"FH",3.988734},{"FI",0.950947},{"FK",3.386458},{"FL",4.031303},{"FM",1.452033},
	{"FN",2.555269},{"FP",4.987153},{"FQ",5.749722},{"FR",3.720376},{"FS",7.700617},{"FT",2.106015},{"FV",3.404174},{"FW",2.314627},{"FY",2.548921},
	{"G-",3.159415},{"GA",4.259660},{"GC",4.096022},{"GD",7.653339},{"GE",4.685478},{"GF",3.248554},{"GG",0.000000},{"GH",4.973258},{"GI",2.862307},
	{"GK",3.513574},{"GL",4.930933},{"GM",3.985662},{"GN",2.281995},{"GP",4.644438},{"GQ",6.449124},{"GR",2.777967},{"GS",7.713728},{"GT",1.794826},
	{"GV",4.381997},{"GW",4.278271},{"GY",2.844205},{"H-",2.290895},{"HA",3.430656},{"HC",2.122616},{"HD",4.053677},{"HE",3.885589},{"HF",3.988734},
	{"HG",4.973258},{"HH",0.000000},{"HI",4.606832},{"HK",3.389498},{"HL",3.224376},{"HM",4.383811},{"HN",3.669550},{"HP",3.400338},{"HQ",2.505015},
	{"HR",4.913960},{"HS",4.936588},{"HT",4.832215},{"HV",3.585443},{"HW",2.643804},{"HY",5.219847},{"I-",2.683561},{"IA",3.390855},{"IC",3.625603},
	{"ID",7.509780},{"IE",3.290365},{"IF",0.950947},{"IG",2.862307},{"IH",4.606832},{"II",0.000000},{"IK",3.622541},{"IL",4.154383},{"IM",1.843231},
	{"IN",2.777607},{"IP",5.290028},{"IQ",6.259904},{"IR",3.562906},{"IS",8.025833},{"IT",1.659940},{"IV",3.501528},{"IW",3.194558},{"IY",2.661635},
	{"K-",2.596979},{"KA",3.828067},{"KC",4.151819},{"KD",6.543439},{"KE",2.875361},{"KF",3.386458},{"KG",3.513574},{"KH",3.389498},{"KI",3.622541},
	{"KK",0.000000},{"KL",4.628726},{"KM",3.271743},{"KN",1.957013},{"KP",4.904732},{"KQ",5.077834},{"KR",1.831912},{"KS",7.447268},{"KT",2.933564},
	{"KV",4.695817},{"KW",3.608435},{"KY",3.366764},{"L-",2.596459},{"LA",1.247798},{"LC",2.884978},{"LD",4.282674},{"LE",4.020771},{"LF",4.031303},
	{"LG",4.930933},{"LH",3.224376},{"LI",4.154383},{"LK",4.628726},{"LL",0.000000},{"LM",4.900633},{"LN",4.533056},{"LP",3.443298},{"LQ",3.274920},
	{"LR",5.628819},{"LS",4.504032},{"LT",4.649613},{"LV",1.292594},{"LW",4.245197},{"LY",6.042466},{"M-",3.187256},{"MA",4.154732},{"MC",4.008728},
	{"MD",7.821502},{"ME",3.176445},{"MF",1.452033},{"MG",3.985662},{"MH",4.383811},{"MI",1.843231},{"MK",3.271743},{"ML",4.900633},{"MM",0.000000},
	{"MN",3.121698},{"MP",6.108183},{"MQ",6.426531},{"MR",3.528257},{"MS",8.669123},{"MT",2.741514},{"MV",4.558618},{"MW",2.816647},{"MY",2.694847},
	{"N-",2.043331},{"NA",3.901718},{"NC",3.489685},{"ND",6.503038},{"NE",2.934314},{"NF",2.555269},{"NG",2.281995},{"NH",3.669550},{"NI",2.777607},
	{"NK",1.957013},{"NL",4.533056},{"NM",3.121698},{"NN",0.000000},{"NP",4.056538},{"NQ",5.237738},{"NR",2.330923},{"NS",7.208530},{"NT",1.830109},
	{"NV",4.079951},{"NW",2.923269},{"NY",2.121650},{"P-",3.021241},{"PA",3.962688},{"PC",2.979329},{"PD",3.451637},{"PE",4.888814},{"PF",4.987153},
	{"PG",4.644438},{"PH",3.400338},{"PI",5.290028},{"PK",4.904732},{"PL",3.443298},{"PM",6.108183},{"PN",4.056538},{"PP",0.000000},{"PQ",2.935575},
	{"PR",5.882474},{"PS",3.476464},{"PT",5.038512},{"PV",3.128562},{"PW",4.262112},{"PY",5.825624},{"Q-",3.691869},{"QA",3.992255},{"QC",3.605052},
	{"QD",1.630031},{"QE",4.832194},{"QF",5.749722},{"QG",6.449124},{"QH",2.505015},{"QI",6.259904},{"QK",5.077834},{"QL",3.274920},{"QM",6.426531},
	{"QN",5.237738},{"QP",2.935575},{"QQ",0.000000},{"QR",6.646255},{"QS",2.985532},{"QT",6.356398},{"QV",3.826160},{"QW",4.622251},{"QY",7.085640},
	{"R-",3.637142},{"RA",4.650462},{"RC",5.124656},{"RD",8.073717},{"RE",4.007668},{"RF",3.720376},{"RG",2.777967},{"RH",4.913960},{"RI",3.562906},
	{"RK",1.831912},{"RL",5.628819},{"RM",3.528257},{"RN",2.330923},{"RP",5.882474},{"RQ",6.646255},{"RR",0.000000},{"RS",8.677989},{"RT",2.422829},
	{"RV",5.518152},{"RW",4.622196},{"RY",2.925919},{"S-",5.669383},{"SA",5.542608},{"SC",5.086767},{"SD",2.295953},{"SE",7.292318},{"SF",7.700617},
	{"SG",7.713728},{"SH",4.936588},{"SI",8.025833},{"SK",7.447268},{"SL",4.504032},{"SM",8.669123},{"SN",7.208530},{"SP",3.476464},{"SQ",2.985532},
	{"SR",8.677989},{"SS",0.000000},{"ST",8.093516},{"SV",4.911201},{"SW",6.744983},{"SY",9.051536},{"T-",2.745560},{"TA",3.765807},{"TC",4.165381},
	{"TD",7.595762},{"TE",3.300894},{"TF",2.106015},{"TG",1.794826},{"TH",4.832215},{"TI",1.659940},{"TK",2.933564},{"TL",4.649613},{"TM",2.741514},
	{"TN",1.830109},{"TP",5.038512},{"TQ",6.356398},{"TR",2.422829},{"TS",8.093516},{"TT",0.000000},{"TV",4.045306},{"TW",3.776321},{"TY",2.056380},
	{"V-",2.300174},{"VA",1.726268},{"VC",2.602403},{"VD",4.685435},{"VE",3.767307},{"VF",3.404174},{"VG",4.381997},{"VH",3.585443},{"VI",3.501528},
	{"VK",4.695817},{"VL",1.292594},{"VM",4.558618},{"VN",4.079951},{"VP",3.128562},{"VQ",3.826160},{"VR",5.518152},{"VS",4.911201},{"VT",4.045306},
	{"VV",0.000000},{"VW",3.823493},{"VY",5.388970},{"W-",2.319116},{"WA",4.167385},{"WC",2.082931},{"WD",5.866515},{"WE",3.475989},{"WF",2.314627},
	{"WG",4.278271},{"WH",2.643804},{"WI",3.194558},{"WK",3.608435},{"WL",4.245197},{"WM",2.816647},{"WN",2.923269},{"WP",4.262112},{"WQ",4.622251},
	{"WR",4.622196},{"WS",6.744983},{"WT",3.776321},{"WV",3.823493},{"WW",0.000000},{"WY",3.440509},{"Y-",3.654066},{"YA",5.337790},{"YC",4.640743},
	{"YD",8.336756},{"YE",3.931641},{"YF",2.548921},{"YG",2.844205},{"YH",5.219847},{"YI",2.661635},{"YK",3.366764},{"YL",6.042466},{"YM",2.694847},
	{"YN",2.121650},{"YP",5.825624},{"YQ",7.085640},{"YR",2.925919},{"YS",9.051536},{"YT",2.056380},{"YV",5.388970},{"YW",3.440509},{"YY",0.000000}
};

std::unordered_map<std::string,std::string> gc = {
        {"TAA","*"},{"TAG","*"},{"TAR","*"},{"TGA","*"},{"TRA","*"},{"GCA","A"},{"GCB","A"},{"GCC","A"},{"GCD","A"},{"GCG","A"},{"GCH","A"},
        {"GCK","A"},{"GCM","A"},{"GCN","A"},{"GCR","A"},{"GCS","A"},{"GCT","A"},{"GCV","A"},{"GCW","A"},{"GCY","A"},{"TGC","C"},{"TGT","C"},
        {"TGY","C"},{"GAC","D"},{"GAT","D"},{"GAY","D"},{"GAA","E"},{"GAG","E"},{"GAR","E"},{"TTC","F"},{"TTT","F"},{"TTY","F"},{"GGA","G"},
        {"GGB","G"},{"GGC","G"},{"GGD","G"},{"GGG","G"},{"GGH","G"},{"GGK","G"},{"GGM","G"},{"GGN","G"},{"GGR","G"},{"GGS","G"},{"GGT","G"},
        {"GGV","G"},{"GGW","G"},{"GGY","G"},{"CAC","H"},{"CAT","H"},{"CAY","H"},{"ATA","I"},{"ATC","I"},{"ATH","I"},{"ATM","I"},{"ATT","I"},
        {"ATW","I"},{"ATY","I"},{"AAA","K"},{"AAG","K"},{"AAR","K"},{"CTA","L"},{"CTB","L"},{"CTC","L"},{"CTD","L"},{"CTG","L"},{"CTH","L"},
        {"CTK","L"},{"CTM","L"},{"CTN","L"},{"CTR","L"},{"CTS","L"},{"CTT","L"},{"CTV","L"},{"CTW","L"},{"CTY","L"},{"TTA","L"},{"TTG","L"},
        {"TTR","L"},{"YTA","L"},{"YTG","L"},{"YTR","L"},{"ATG","M"},{"AAC","N"},{"AAT","N"},{"AAY","N"},{"CCA","P"},{"CCB","P"},{"CCC","P"},
        {"CCD","P"},{"CCG","P"},{"CCH","P"},{"CCK","P"},{"CCM","P"},{"CCN","P"},{"CCR","P"},{"CCS","P"},{"CCT","P"},{"CCV","P"},{"CCW","P"},
        {"CCY","P"},{"CAA","Q"},{"CAG","Q"},{"CAR","Q"},{"AGA","R"},{"AGG","R"},{"AGR","R"},{"CGA","R"},{"CGB","R"},{"CGC","R"},{"CGD","R"},
        {"CGG","R"},{"CGH","R"},{"CGK","R"},{"CGM","R"},{"CGN","R"},{"CGR","R"},{"CGS","R"},{"CGT","R"},{"CGV","R"},{"CGW","R"},{"CGY","R"},
        {"MGA","R"},{"MGG","R"},{"MGR","R"},{"AGC","S"},{"AGT","S"},{"AGY","S"},{"TCA","S"},{"TCB","S"},{"TCC","S"},{"TCD","S"},{"TCG","S"},
        {"TCH","S"},{"TCK","S"},{"TCM","S"},{"TCN","S"},{"TCR","S"},{"TCS","S"},{"TCT","S"},{"TCV","S"},{"TCW","S"},{"TCY","S"},{"ACA","T"},
        {"ACB","T"},{"ACC","T"},{"ACD","T"},{"ACG","T"},{"ACH","T"},{"ACK","T"},{"ACM","T"},{"ACN","T"},{"ACR","T"},{"ACS","T"},{"ACT","T"},
        {"ACV","T"},{"ACW","T"},{"ACY","T"},{"GTA","V"},{"GTB","V"},{"GTC","V"},{"GTD","V"},{"GTG","V"},{"GTH","V"},{"GTK","V"},{"GTM","V"},
        {"GTN","V"},{"GTR","V"},{"GTS","V"},{"GTT","V"},{"GTV","V"},{"GTW","V"},{"GTY","V"},{"TGG","W"},{"TAC","Y"},{"TAT","Y"},{"TAY","Y"},
        {"---","-"},{"...","."},{"~~~","~"}
};

std::unordered_set<std::string> ambig_equal = {
        "AM","MA","CM","MC","AV","VA","CV","VC","GV","VG","AN","NA","CN","NC","GN","NG",
        "TN","NT","AH","HA","CH","HC","TH","HT","AR","RA","GR","RG","AD","DA","GD","DG",
        "TD","DT","AW","WA","TW","WT","CS","SC","GS","SG","CB","BC","GB","BG","TB","BT",
        "CY","YC","TY","YT","GK","KG","TK","KT"
};


// Utility functions
// Compare alleles for two strings.
bool comp_allele (std::string s1,std::string s2) { 
	int x = 0;
	int y = 0;
	int index = 0;
	
	std::string buff1 = "";
	for( index = 0; index < s1.length(); index++ ) {
		if ( isdigit(s1[index]) ) { buff1 += s1[index]; }
		else if ( !buff1.empty() ) { break; }
	}
	std::istringstream(buff1) >> x;

	std::string buff2 = "";
	for( index = 0; index < s2.length(); index++ ) {
		if ( isdigit(s2[index]) ) { buff2 += s2[index]; }
		else if ( !buff2.empty() ) { break; }
	}
	std::istringstream(buff2) >> y;

	if ( x < y ) {
		return true;
	} else if ( y < x ) {
		return false;
	} else {
		return ( s1 < s2 );
	}
}

// Inline functions
inline std::vector<int> split_set_by_substr(const std::string& str, const std::string& delim) {
	std::unordered_set<std::string> tokens;
	std::size_t prev = 0;
	std::size_t pos = 0;

	if ( delim.length() == 0 ) {
		for(std::size_t k = 0;k < str.length();k++) {
			tokens.insert(str.substr(k,1));
		}
	} else {
		do {
			pos = str.find(delim, prev);
			if (pos == std::string::npos) pos = str.length();
			std::string token = str.substr(prev, pos-prev);
			if (!token.empty()) { 
				tokens.insert( token );
			}
			prev = pos + delim.length();
		} while (pos < str.length() && prev < str.length());
	}

	std::vector<int> v; int num;
	for ( auto it = tokens.begin(); it != tokens.end(); ++it ) {
		try { num = std::stoi(*it); } catch(...) { continue; }
		v.push_back( num );
	}
	return v;
}


// Courtesy https://stackoverflow.com/questions/14265581/parse-split-a-string-in-c-using-string-delimiter-standard-c
inline std::vector<std::string> split_by_substr(const std::string& str, const std::string& delim) {
	std::vector<std::string> tokens;
	std::size_t prev = 0;
	std::size_t pos = 0;

	if ( delim.length() == 0 ) {
		for(std::size_t k = 0;k < str.length();k++) {
			tokens.push_back(str.substr(k,1));
		}
	} else {
		do {
			pos = str.find(delim, prev);
			if (pos == std::string::npos) pos = str.length();
			std::string token = str.substr(prev, pos-prev);
			if (!token.empty()) tokens.push_back(token);
			prev = pos + delim.length();
		} while (pos < str.length() && prev < str.length());
	}
	return tokens;
}

inline StringVal to_StringVal(FunctionContext* context, const std::string& s) {
	if ( s.size() > StringVal::MAX_LENGTH ) {
		return StringVal::null();
	} else  {
		StringVal result(context, s.size());
		memcpy(result.ptr, s.c_str(), s.size());
		return result;
	}
}
// Finished with inlines


// We take a string of delimited values in a string and sort it in ascending order
IMPALA_UDF_EXPORT
StringVal Sort_List_By_Substring(FunctionContext* context, const StringVal& listVal, const StringVal& delimVal ) {
	if ( listVal.is_null || delimVal.is_null ) { return StringVal::null(); }
	if ( listVal.len == 0 || delimVal.len == 0 ) { return listVal; };
	
	std::string list  ((const char *)listVal.ptr,listVal.len);
	std::string delim ((const char *)delimVal.ptr,delimVal.len);
	std::vector<std::string> tokens = split_by_substr(list,delim);

	if ( tokens.size() == 0 ) {
		return listVal;
	} else {
		// Use the usual ascending sort
		std::sort(tokens.begin(), tokens.end());
		std::string s = tokens[0];
		for ( std::vector<std::string>::const_iterator i = tokens.begin() +1; i < tokens.end(); ++i) { s += delim + *i; }

		return to_StringVal(context, s);
	}
}

// We take a string of delimited values in a string and sort it in ascending order
IMPALA_UDF_EXPORT
StringVal Range_From_List(FunctionContext* context, const StringVal& listVal, const StringVal& delimVal ) {
	if ( listVal.is_null || delimVal.is_null ) { return StringVal::null(); }
	if ( listVal.len == 0 || delimVal.len == 0 ) { return listVal; };
	
	std::string list  ((const char *)listVal.ptr,listVal.len);
	std::string delim ((const char *)delimVal.ptr,delimVal.len);
	std::vector<int> tokens = split_set_by_substr(list,delim);
	std::sort( tokens.begin(), tokens.end() );

	if ( tokens.size() == 0 ) {
		return StringVal::null();
	} else {
		std::vector<int>::iterator it = tokens.begin();
		std::string s = std::to_string(*it); 
		int previous = *it; ++it;
		int range = 0;
		
		while( it!=tokens.end() ) {
			if ( *it == (previous+1) ) {
				range = 1;
			} else {
				if ( range ) {
					range = 0;
					s += ".." + std::to_string(previous);
					s += delim + std::to_string(*it);
				} else {
					s += delim + std::to_string(*it);
				}
			}
			previous = *it; ++it;
		}

		if ( range ) {
			s += ".." + std::to_string(previous);
		}

		return to_StringVal(context, s);
	}
}

// We take a string of delimited values in a string and sort it in ascending order
IMPALA_UDF_EXPORT
StringVal Sort_List_By_Substring_Unique(FunctionContext* context, const StringVal& listVal, const StringVal& delimVal ) {
	if ( listVal.is_null || delimVal.is_null ) { return StringVal::null(); }
	if ( listVal.len == 0 || delimVal.len == 0 ) { return listVal; };
	
	std::string list  ((const char *)listVal.ptr,listVal.len);
	std::string delim ((const char *)delimVal.ptr,delimVal.len);
	std::vector<std::string> tokens = split_by_substr(list,delim);

	if ( tokens.size() == 0 ) {
		if ( list == delim ) {
			return StringVal("");
		} else {
			return listVal;
		}
	} else {
		// Use the usual ascending sort
		std::sort(tokens.begin(), tokens.end());
		std::string s = tokens[0];
		for ( std::size_t i = 1; i < tokens.size(); i++) {
			if ( tokens[i] != tokens[i-1] ) {
				s += delim + tokens[i];
			}
		}

		return to_StringVal(context, s);
	}
}

IMPALA_UDF_EXPORT
StringVal Sort_List_By_Set(FunctionContext* context, const StringVal& listVal, const StringVal& delimVal, const StringVal& outDelimVal ) {
	if ( listVal.is_null || delimVal.is_null || outDelimVal.is_null ) { return StringVal::null(); }
	if ( listVal.len == 0 || delimVal.len == 0) { return listVal; };
	
	std::vector<std::string> tokens;
	std::string list  ((const char *)listVal.ptr,listVal.len);
	std::string delim ((const char *)delimVal.ptr,delimVal.len);

	std::string odelim = "";
	if ( outDelimVal.len > 0 ) {
		odelim.assign ((const char *)outDelimVal.ptr,outDelimVal.len);
	}

	// Initialize positions
	std::string::size_type lastPos = list.find_first_not_of(delim, 0);
	std::string::size_type pos     = list.find_first_of(delim, lastPos);
	while (std::string::npos != pos || std::string::npos != lastPos) {
		tokens.push_back(list.substr(lastPos, pos - lastPos));
		lastPos = list.find_first_not_of(delim, pos);
		pos = list.find_first_of(delim, lastPos);
	}

	// Use the usual ascending sort
	std::sort(tokens.begin(), tokens.end());
	std::string s = tokens[0];
	for ( std::vector<std::string>::const_iterator i = tokens.begin() +1; i < tokens.end(); ++i) { s += odelim + *i; }

	return to_StringVal(context, s);
}

// We take a string of delimited values in a string and sort it in ascending order
IMPALA_UDF_EXPORT
StringVal Sort_Allele_List(FunctionContext* context, const StringVal& listVal, const StringVal& delimVal ) {
	if ( listVal.is_null || delimVal.is_null ) { return StringVal::null(); }
	if ( listVal.len == 0 || delimVal.len == 0 ) { return listVal; };

	std::string list  ((const char *)listVal.ptr,listVal.len);
	std::string delim ((const char *)delimVal.ptr,delimVal.len);
	std::vector<std::string> tokens = split_by_substr(list,delim);

	if ( tokens.size() == 0 ) {
		return listVal;
	} else {
		// Use the usual ascending sort
		std::sort(tokens.begin(), tokens.end(), comp_allele);
		std::string s = tokens[0];
		for ( std::vector<std::string>::const_iterator i = tokens.begin() +1; i < tokens.end(); ++i) { s += delim + *i; }

		return to_StringVal(context,s);
	}
}

IMPALA_UDF_EXPORT
BooleanVal Find_Set_In_String(FunctionContext* context, const StringVal& haystackVal, const StringVal& needlesVal ) {
	// check for nulls
	if ( haystackVal.is_null || needlesVal.is_null ) { 
		return BooleanVal::null(); 
	// haystack and needles not null
	} else if ( haystackVal.len == 0 || needlesVal.len == 0 ) { 
		// Can't find something in nothing or vice-versa
		if ( haystackVal.len != needlesVal.len ) {
			return BooleanVal(false);
		// Special case that differs from instr
		// letting empty set be found in an empty string
		} else {
			return BooleanVal(true);
		}
	// haystack and needles are non-trivial
	} else {
		std::string haystack ((const char *)haystackVal.ptr,haystackVal.len);
		std::string needles  ((const char *)needlesVal.ptr,needlesVal.len);
		if ( haystack.find_first_of(needles) != std::string::npos ) {
			return BooleanVal(true);
		} else {
			return BooleanVal(false);
		}
	}
}


// We take codon(s) and translate it/them
IMPALA_UDF_EXPORT
StringVal To_AA(FunctionContext* context, const StringVal& ntsVal ) {
	if ( ntsVal.is_null ) { return StringVal::null(); }
	if ( ntsVal.len == 0 ) { return ntsVal; };

	std::string bases ((const char *)ntsVal.ptr,ntsVal.len);
	std::string residues = "";
	std::string aa = "";
	std::string codon = "";
	std::locale loc;

	// Initialize positions
	long unsigned int N = bases.length();
	long unsigned int R = N % 3;
	long unsigned int codon_index = 0;

	for(codon_index=0;codon_index+2<N;codon_index+=3) {
		// get codon and ignore case
		codon = bases.substr(codon_index,3);
		for (std::string::size_type i=0; i<3; ++i) {
    			codon[i] = std::toupper(codon[i],loc);
		}

		if ( gc.count(codon) > 0 ) {
			aa = gc[codon];
		} else if ( codon.find_first_of(".-~") != std::string::npos ) {
			aa = "~";
		} else if ( codon.find_first_not_of("ACGTURYSWKMBDHVN") != std::string::npos ) {
			aa = "?";
		} else {
			aa = "X";
		}
		residues += aa;
	}

	if ( R > 0 ) {
		residues += "?";
	}

	return to_StringVal(context,residues);
}

IMPALA_UDF_EXPORT
StringVal To_AA_Mutant(FunctionContext* context, const StringVal& ntsVal, const StringVal& alleleVal, const IntVal& pos ) {
	if ( ntsVal.is_null || alleleVal.is_null || pos.is_null ) { return StringVal::null(); }
	if ( alleleVal.len == 0 ) { 
		return To_AA(context,ntsVal);
	} else if ( ntsVal.len == 0 ) { 
		return To_AA(context,alleleVal);
	}

	std::string bases ((const char *)ntsVal.ptr,ntsVal.len);
	std::string allele ((const char *)alleleVal.ptr,alleleVal.len);

	if ( pos.val < 1 ) {
		bases = allele + bases;
	} else if ( pos.val > ntsVal.len ) {
		bases = bases + allele;
	} else if ( (pos.val + alleleVal.len - 1) > ntsVal.len ) {
		bases.replace(pos.val-1,ntsVal.len - pos.val + 1,allele);
	} else {
		bases.replace(pos.val-1,allele.size(),allele);
	}

	// Copy sorted string to StringVal structure
	StringVal result(context, bases.size());
	memcpy(result.ptr, bases.c_str(), bases.size());
	return To_AA(context,result);
}

// Take the reverse complement of the nucleotide string
IMPALA_UDF_EXPORT
StringVal Rev_Complement(FunctionContext* context, const StringVal& ntsVal ) {
	if ( ntsVal.is_null ) { return StringVal::null(); }
	if ( ntsVal.len == 0 ) { return ntsVal; };

	std::string seq ((const char *)ntsVal.ptr,ntsVal.len);
	reverse(seq.begin(), seq.end());
	int was_lower = 0;
	for (std::size_t i = 0; i < seq.length(); i++) {
		if ( islower(seq[i]) ) {
			seq[i] -= 32;	
			was_lower = 1;
		} else {
			was_lower = 0;
		}

		if( seq[i] == 'G' ) {
			seq[i] = 'C';
		} else if(seq[i] == 'C') {
			seq[i] = 'G';
		} else if(seq[i] == 'A') {
			seq[i] = 'T';
		} else if(seq[i] == 'T' ) {
			seq[i] = 'A';
		} else if(seq[i] == 'R' ) {
			seq[i] = 'Y';
		} else if(seq[i] == 'Y' ) {
			seq[i] = 'R';
		} else if(seq[i] == 'K' ) {
			seq[i] = 'M';
		} else if(seq[i] == 'M' ) {
			seq[i] = 'K';
		} else if(seq[i] == 'B' ) {
			seq[i] = 'V'; 
		} else if(seq[i] == 'V' ) {
			seq[i] = 'B';
		} else if(seq[i] == 'D' ) {
			seq[i] = 'H';
		} else if(seq[i] == 'H' ) {
			seq[i] = 'D';
		} else if(seq[i] == 'U' ) {
			seq[i] = 'A';
		}
 
		if ( was_lower ) { seq[i] += 32; }
	}
	return to_StringVal(context,seq);
}

IMPALA_UDF_EXPORT
StringVal Complete_String_Date(FunctionContext* context, const StringVal& dateStr ) {
	if ( dateStr.is_null  || dateStr.len == 0 ) { return StringVal::null(); }
	std::string date ((const char *)dateStr.ptr,dateStr.len);
	std::vector<std::string> tokens;
	boost::split(tokens, date, boost::is_any_of("-/."));
	std::string buffer = "";

	if ( tokens.size() >= 3 ) {
		buffer = tokens[0] + "-" + tokens[1] + "-" + tokens[2];
	} else if ( tokens.size() == 2 ) {
		buffer = tokens[0] + "-" + tokens[1] + "-01";
	} else if ( tokens.size() == 1 ) {
		buffer = tokens[0] + "-01-01";
	} else {
		return StringVal::null(); 
	}

	return to_StringVal(context,buffer);
}


// Convert Grogorian Dates to the EPI (MMWR) Week
// See: https://wwwn.cdc.gov/nndss/document/MMWR_Week_overview.pdf
struct epiweek_t date_to_epiweek( boost::gregorian::date d ) {
	// Boost starts with Sunday.
	int day_of_year 	= d.day_of_year();
	int weekday		= d.day_of_week();

	boost::gregorian::date 	start_date(d.year(),1,1);
	int start_weekday 	= start_date.day_of_week();
	boost::gregorian::date 	next_year_date(d.year()+1,1,1);
	int next_year_weekday 	= next_year_date.day_of_week();

	// December & 29 - 31 &  Sun-Tues & Next year is Sun-Thu
	if ( d.month() == 12 && d.day() > 28 && weekday < 3 && next_year_weekday < 4 ) {
		struct epiweek_t result =  {d.year()+1, 1};
		return result;
	} 

	int epiweek 	= ( day_of_year + (start_weekday - 1) ) / 7;
	// Sunday, Monday, Tuesday, Wednesday
	if ( start_weekday < 4 ) {
		epiweek++;
	}

	if ( epiweek > 0 ) {
		struct epiweek_t result = {d.year(), epiweek};
		return result;
	} else {
		boost::gregorian::date 	last_year_date(d.year()-1,12,31);
		return date_to_epiweek(last_year_date);	
	}
}

IMPALA_UDF_EXPORT
IntVal Convert_Timestamp_To_EPI_Week(FunctionContext* context, const TimestampVal& tsVal ) {
	return Convert_Timestamp_To_EPI_Week(context,tsVal,BooleanVal(false));
}
    
IMPALA_UDF_EXPORT
IntVal Convert_Timestamp_To_EPI_Week(FunctionContext* context, const TimestampVal& tsVal, const BooleanVal& yearFormat ) {
	if ( tsVal.is_null || yearFormat.is_null ) { return IntVal::null(); }

	try {
		boost::gregorian::date d( tsVal.date );
		struct epiweek_t epi = date_to_epiweek(d);
		if ( yearFormat.val ) { 
			return IntVal( epi.year * 100 + epi.week );
		} else {
			return IntVal(epi.week);
		}
	} catch (const boost::exception& e) {
		return IntVal::null();
	} catch (...) {
		return IntVal::null();
	} 
}

IMPALA_UDF_EXPORT
IntVal Convert_String_To_EPI_Week(FunctionContext* context, const StringVal& dateStr ) {
	return Convert_String_To_EPI_Week(context,dateStr,BooleanVal(false));
}


IMPALA_UDF_EXPORT
IntVal Convert_String_To_EPI_Week(FunctionContext* context, const StringVal& dateStr, const BooleanVal& yearFormat ) {
	if ( dateStr.is_null  || dateStr.len == 0 || yearFormat.is_null ) { return IntVal::null(); }
	std::string date ((const char *)dateStr.ptr,dateStr.len);
	std::vector<std::string> tokens;
	boost::split(tokens, date, boost::is_any_of("-/."));

	try {
		int year, month, day;
		if ( tokens.size() >= 3 ) {
			year 	= std::stoi(tokens[0]);
			month 	= std::stoi(tokens[1]);
			day 	= std::stoi(tokens[2]);
		} else if ( tokens.size() == 2 ) {
			year 	= std::stoi(tokens[0]);
			month 	= std::stoi(tokens[1]);
			day 	= 1;
		} else if ( tokens.size() == 1 ) {
			year 	= std::stoi(tokens[0]);
			month 	= 1;
			day 	= 1;
		} else {
			return IntVal::null(); 
		}

		boost::gregorian::date d(year,month,day);
		struct epiweek_t epi = date_to_epiweek(d);
		if ( yearFormat.val ) { 
			return IntVal( epi.year * 100 + epi.week );
		} else {
			return IntVal( epi.week );
		}
	} catch (const boost::exception& e) {
		return IntVal::null();
	} catch(std::invalid_argument& e) {
		return IntVal::null();
  	} catch(std::out_of_range& e) {
		return IntVal::null();
	} catch (...) {
		return IntVal::null();
	} 
}

IMPALA_UDF_EXPORT
StringVal Substring_By_Range(FunctionContext* context, const StringVal& sequence, const StringVal& rangeMap ) {
	if ( sequence.is_null  || sequence.len == 0 || rangeMap.is_null || rangeMap.len == 0 ) { return StringVal::null(); }

	std::string seq ((const char *)sequence.ptr,sequence.len);
	std::string map ((const char *)rangeMap.ptr,rangeMap.len);
	std::string buffer = "";
	std::vector<std::string> tokens;
	int L = seq.length();
	int x,a,b;

	boost::split(tokens, map, boost::is_any_of(";,"));
	for(int i = 0; i < tokens.size(); i++ ) {
		if ( tokens[i].find("..") != std::string::npos ) {
			std::vector<std::string> range = split_by_substr(tokens[i],"..");
			if ( range.size() == 0 ) { return StringVal::null(); }	

			try {
				a = std::stoi(range[0]) - 1;
				b = std::stoi(range[1]) - 1;
			} catch (...) { 
				return StringVal::null();
			}

			if ( b >= L )	{ b = L - 1; }
			if ( a >= L )	{ a = L - 1; }
			if ( a < 0 )	{ a = 0; }
			if ( b < 0 )	{ b = 0; }

			if ( a <= b ) { 
				for( int j = a; j <= b; j++ ) {
					buffer += seq[j];
				}
			} else {
				for( int j = a; j >= b; j-- ) {
					buffer += seq[j];
				}
			}
		} else {
			try {
				x = std::stoi(tokens[i]) - 1;
			} catch(...) {
				return StringVal::null();
			}

			if ( x < L && x >= 0 ) {
				buffer += seq[x];
			}
		}
	}

	return to_StringVal(context,buffer);
}

// Create a mutation list from two aligned strings
IMPALA_UDF_EXPORT
StringVal Mutation_List_Strict(FunctionContext* context, const StringVal& sequence1, const StringVal& sequence2 ) {
	if ( sequence1.is_null  || sequence2.is_null  ) { return StringVal::null(); }
	if ( sequence1.len == 0 || sequence2.len == 0 ) { return StringVal::null(); };

	std::size_t length = sequence1.len;
	if ( sequence2.len < sequence1.len ) {
		length = sequence2.len;
	}

	std::string seq1 ((const char *)sequence1.ptr,sequence1.len);
	std::string seq2 ((const char *)sequence2.ptr,sequence2.len);
	std::string buffer = "";

	for (std::size_t i = 0; i < length; i++) {
		if ( seq1[i] != seq2[i] ) {
			seq1[i] = toupper(seq1[i]);
			seq2[i] = toupper(seq2[i]);
			if ( seq1[i] != seq2[i] ) {
				if ( seq1[i] != '.' && seq2[i] != '.' ) {
					if ( buffer.length() > 0 ) {
						buffer += ", ";
						buffer += seq1[i];
						buffer += boost::lexical_cast<std::string>(i+1);
						buffer += seq2[i];
					} else {
						buffer = seq1[i] + boost::lexical_cast<std::string>(i+1) + seq2[i];
					}
				}
			}
		}
	}

	return to_StringVal(context,buffer);
}

// Create a mutation list from two aligned strings
IMPALA_UDF_EXPORT
StringVal Mutation_List_PDS(FunctionContext* context, const StringVal& sequence1, const StringVal& sequence2, const StringVal& pairwise_delete_set ) {
	if ( sequence1.is_null  || sequence2.is_null || pairwise_delete_set.is_null  ) { return StringVal::null(); }
	if ( sequence1.len == 0 || sequence2.len == 0 ) { return StringVal::null(); };

	std::size_t length = sequence1.len;
	if ( sequence2.len < sequence1.len ) {
		length = sequence2.len;
	}

	std::string seq1 ((const char *)sequence1.ptr,sequence1.len);
	std::string seq2 ((const char *)sequence2.ptr,sequence2.len);
	std::string buffer = "";
	std::unordered_map<char,int> m;
	if ( pairwise_delete_set.len > 0 ) {
		std::string dset ((const char *)pairwise_delete_set.ptr,pairwise_delete_set.len);
		for (std::size_t i = 0; i < pairwise_delete_set.len; i++) {
			m[dset[i]] = 1;
		}
	}

	for (std::size_t i = 0; i < length; i++) {
		if ( seq1[i] != seq2[i] ) {
			seq1[i] = toupper(seq1[i]);
			seq2[i] = toupper(seq2[i]);
			if ( seq1[i] != seq2[i] ) {
				if ( m.count( seq1[i] ) == 0 && m.count( seq2[i] ) == 0 ) {
					if ( buffer.length() > 0 ) {
						buffer += ", ";
						buffer += seq1[i];
						buffer += boost::lexical_cast<std::string>(i+1);
						buffer += seq2[i];
					} else {
						buffer = seq1[i] + boost::lexical_cast<std::string>(i+1) + seq2[i];
					}
				}
			}
		}
	}

	return to_StringVal(context,buffer);
}

IMPALA_UDF_EXPORT
StringVal Mutation_List_Strict(FunctionContext* context, const StringVal& sequence1, const StringVal& sequence2, const StringVal& rangeMap ) {
	if ( sequence1.is_null  || sequence2.is_null || rangeMap.is_null  ) { return StringVal::null(); }
	if ( sequence1.len == 0 || sequence2.len == 0 || rangeMap.len == 0 ) { return StringVal::null(); };

	std::size_t length = sequence1.len;
	if ( sequence2.len < sequence1.len ) {
		length = sequence2.len;
	}

	std::string seq1 ((const char *)sequence1.ptr,sequence1.len);
	std::string seq2 ((const char *)sequence2.ptr,sequence2.len);
	std::string map ((const char *)rangeMap.ptr,rangeMap.len);

	int x,a,b;
	int L = length;
	std::vector<int> sites;
	std::vector<std::string> tokens;
	boost::split(tokens, map, boost::is_any_of(";,"));
	for(int i = 0; i < tokens.size(); i++ ) {
		if ( tokens[i].find("..") != std::string::npos ) {
			std::vector<std::string> range = split_by_substr(tokens[i],"..");
			if ( range.size() == 0 ) { return StringVal::null(); }	

			try {
				a = std::stoi(range[0]) - 1;
				b = std::stoi(range[1]) - 1;
			} catch(...) {
				return StringVal::null();
			}

			if ( b >= L )	{ b = L - 1; }
			if ( a >= L )	{ a = L - 1; }
			if ( a < 0 )	{ a = 0; }
			if ( b < 0 )	{ b = 0; }

			if ( a <= b ) { 
				for( int j = a; j <= b; j++ ) {
					sites.push_back(j);
				}
			} else {
				for( int j = a; j >= b; j-- ) {
					sites.push_back(j);
				}
			}
		} else {
			try {
				x = std::stoi(tokens[i]) - 1;
			} catch (...) {
				return StringVal::null();
			}

			if ( x < L && x >= 0 ) {
				sites.push_back(x);
				
			}
		}
	}

	int pos = 0;
	std::string buffer = "";
	for ( const auto& i : sites ) {
		if ( i < length && i > -1 ) {
			seq1[i] = toupper(seq1[i]);
			seq2[i] = toupper(seq2[i]);
			if ( seq1[i] != seq2[i] ) {
				if ( seq1[i] != '.' && seq2[i] != '.' ) {
					pos = i + 1;
					if ( buffer.length() > 0 ) {
						buffer += std::string(", ") + seq1[i] + std::to_string(pos) + seq2[i];
					} else {
						buffer += seq1[i] + std::to_string(pos) + seq2[i];
					}
				}
			}
		}
	}

	return to_StringVal(context,buffer);
}

// Create a mutation list from two aligned strings
// Add Glycosylation detection
IMPALA_UDF_EXPORT
StringVal Mutation_List_Strict_GLY(FunctionContext* context, const StringVal& sequence1, const StringVal& sequence2 ) {
	if ( sequence1.is_null  || sequence2.is_null  ) { return StringVal::null(); }
	if ( sequence1.len == 0 || sequence2.len == 0 ) { return StringVal::null(); };

	std::size_t length = sequence1.len;
	if ( sequence2.len < sequence1.len ) {
		length = sequence2.len;
	}

	std::string seq1 ((const char *)sequence1.ptr,sequence1.len);
	std::string seq2 ((const char *)sequence2.ptr,sequence2.len);
	std::string buffer = "";

	int add_gly = 0;
	int loss_gly = 0;
	for (std::size_t i = 0; i < length; i++) {
		if ( seq1[i] != seq2[i] ) {
			seq1[i] = toupper(seq1[i]);
			seq2[i] = toupper(seq2[i]);
			if ( seq1[i] != seq2[i] ) {
				if ( seq1[i] != '.' && seq2[i] != '.' ) {
					if ( buffer.length() > 0 ) {
						buffer += ", ";
						buffer += seq1[i];
						buffer += boost::lexical_cast<std::string>(i+1);
						buffer += seq2[i];
					} else {
						buffer = seq1[i] + boost::lexical_cast<std::string>(i+1) + seq2[i];
					}

					
					// GLYCOSYLATION ADD
					add_gly = 0;

					// ~N <= N
					if ( seq2[i] == 'N' ) {
						// CHECK: .[^P][ST]
						if ( (i+2) < length && seq2[i+1] != 'P' && (seq2[i+2] == 'T'||seq2[i+2] == 'S') ) {
							add_gly = 1;
						}
					}

					// P => ~P
					if ( !add_gly && seq1[i] == 'P' ) {
						// CHECK: N.[ST]
						if ( (i+1) < length && i >= 1 && seq2[i-1] == 'N' && (seq2[i+1] == 'T'||seq2[i+1] == 'S') ) {
							add_gly = 1;
						}
					}
 
					// ~[ST] && [ST]
					if ( !add_gly && seq1[i] != 'S' && seq1[i] != 'T' && (seq2[i] == 'S' || seq2[i] == 'T') ) {
						// CHECK: N[^P].
						if ( i >= 2 && seq2[i-2] == 'N' && seq2[i-1] != 'P' ) {
							add_gly = 1;
						}
					}


					// GLYCOSYLATION LOSS
					loss_gly = 0;

					// N => ~N
					if ( seq1[i] == 'N' ) {
						// CHECK: .[^P][ST]
						if ( (i+2) < length && seq1[i+1] != 'P' && (seq1[i+2] == 'T'||seq1[i+2] == 'S') ) {
							loss_gly = 1;
						}
					}

					// ~P <= P
					if ( !loss_gly && seq2[i] == 'P' ) {
						// CHECK: N.[ST]
						if ( (i+1) < length && i >= 1 && seq1[i-1] == 'N' && (seq1[i+1] == 'T'||seq1[i+1] == 'S') ) {
							loss_gly = 1;
						}
					}

					// [ST] && ~[ST]
					if ( !loss_gly && seq2[i] != 'S' && seq2[i] != 'T' && (seq1[i] == 'S' || seq1[i] == 'T') ) {
						// CHECK: N[^P].
						if ( i >= 2 && seq1[i-2] == 'N' && seq1[i-1] != 'P' ) {
							loss_gly = 1;
						}
					}

					if ( add_gly ) {
						buffer += "-ADD";
					}
					if ( loss_gly ) {
						buffer += "-LOSS";
					}
					if ( add_gly || loss_gly ) {
						buffer += "-GLY";
					}
				}
			}
		}
	}

	return to_StringVal(context,buffer);
}


// Create a mutation list from two aligned strings
// Ignore resolvable ambiguations
// NT_distance()
IMPALA_UDF_EXPORT
StringVal Mutation_List_No_Ambiguous(FunctionContext* context, const StringVal& sequence1, const StringVal& sequence2 ) {
	if ( sequence1.is_null  || sequence2.is_null  ) { return StringVal::null(); }
	if ( sequence1.len == 0 || sequence2.len == 0 ) { return StringVal::null(); };

	std::size_t length = sequence1.len;
	if ( sequence2.len < sequence1.len ) {
		length = sequence2.len;
	}

	std::string seq1 ((const char *)sequence1.ptr,sequence1.len);
	std::string seq2 ((const char *)sequence2.ptr,sequence2.len);
	std::string buffer = "";

	for (std::size_t i = 0; i < length; i++) {
		if ( seq1[i] != seq2[i] ) {
			seq1[i] = toupper(seq1[i]);
			seq2[i] = toupper(seq2[i]);
			if ( seq1[i] != seq2[i] ) {
				if ( seq1[i] != '.' && seq2[i] != '.' ) {
					if ( ambig_equal.count( std::string() + seq1[i] + seq2[i] ) == 0 ) {
						if ( buffer.length() > 0 ) {
							buffer += ", ";
							buffer += seq1[i];
							buffer += boost::lexical_cast<std::string>(i+1);
							buffer += seq2[i];
						} else {
							buffer = seq1[i] + boost::lexical_cast<std::string>(i+1) + seq2[i];
						}
					}
				}
			}
		}
	}

	return to_StringVal(context,buffer);
}

IMPALA_UDF_EXPORT
IntVal Hamming_Distance_Pairwise_Delete(FunctionContext* context, const StringVal& sequence1, const StringVal& sequence2, const StringVal& pairwise_delete_set ) {
	if ( sequence1.is_null  || sequence2.is_null || pairwise_delete_set.is_null  ) { return IntVal::null(); }
	if ( sequence1.len == 0 || sequence2.len == 0 ) { return IntVal::null(); };

	std::size_t length = sequence1.len;
	if ( sequence2.len < sequence1.len ) {
		length = sequence2.len;
	}

	std::string seq1 ((const char *)sequence1.ptr,sequence1.len);
	std::string seq2 ((const char *)sequence2.ptr,sequence2.len);
	std::unordered_map<char,int> m;

	if ( pairwise_delete_set.len > 0 ) {
		std::string dset ((const char *)pairwise_delete_set.ptr,pairwise_delete_set.len);
		for (std::size_t i = 0; i < pairwise_delete_set.len; i++) {
			m[dset[i]] = 1;
		}
	}

	int hamming_distance = 0;
	for (std::size_t i = 0; i < length; i++) {
		if ( seq1[i] != seq2[i] ) {
			seq1[i] = toupper(seq1[i]);
			seq2[i] = toupper(seq2[i]);
			if ( seq1[i] != seq2[i] ) {
				if ( m.count( seq1[i] ) == 0 && m.count( seq2[i] ) == 0 ) {
					hamming_distance++;
				}
			}
		}
	}

	return IntVal(hamming_distance);
}

IMPALA_UDF_EXPORT
IntVal Hamming_Distance(FunctionContext* context, const StringVal& sequence1, const StringVal& sequence2 ) {
	if ( sequence1.is_null  || sequence2.is_null  ) { return IntVal::null(); }
	if ( sequence1.len == 0 || sequence2.len == 0 ) { return IntVal::null(); };

	std::size_t length = sequence1.len;
	if ( sequence2.len < sequence1.len ) {
		length = sequence2.len;
	}

	std::string seq1 ((const char *)sequence1.ptr,sequence1.len);
	std::string seq2 ((const char *)sequence2.ptr,sequence2.len);

	int hamming_distance = 0;
	for (std::size_t i = 0; i < length; i++) {
		if ( seq1[i] != seq2[i] ) {
			seq1[i] = toupper(seq1[i]);
			seq2[i] = toupper(seq2[i]);
			if ( seq1[i] != seq2[i] ) {
				if ( seq1[i] != '.' && seq2[i] != '.' ) {
					hamming_distance++;
				}
			}
		}
	}

	return IntVal(hamming_distance);
}

IMPALA_UDF_EXPORT
IntVal Nt_Distance(FunctionContext* context, const StringVal& sequence1, const StringVal& sequence2 ) {
	if ( sequence1.is_null  || sequence2.is_null  ) { return IntVal::null(); }
	if ( sequence1.len == 0 || sequence2.len == 0 ) { return IntVal::null(); };

	std::size_t length = sequence1.len;
	if ( sequence2.len < sequence1.len ) {
		length = sequence2.len;
	}

	std::string seq1 ((const char *)sequence1.ptr,sequence1.len);
	std::string seq2 ((const char *)sequence2.ptr,sequence2.len);

	int hamming_distance = 0;
	for (std::size_t i = 0; i < length; i++) {
		if ( seq1[i] != seq2[i] ) {
			seq1[i] = toupper(seq1[i]);
			seq2[i] = toupper(seq2[i]);
			if ( seq1[i] != seq2[i] ) {
				if ( seq1[i] != '.' && seq2[i] != '.' ) {
					if ( ambig_equal.count( std::string() + seq1[i] + seq2[i] ) == 0 ) {
						hamming_distance++;
					}
				}
			}
		}
	}

	return IntVal(hamming_distance);
}

IMPALA_UDF_EXPORT
DoubleVal Physiochemical_Distance(FunctionContext* context, const StringVal& sequence1, const StringVal& sequence2 ) {
	if ( sequence1.is_null  || sequence2.is_null  ) { return DoubleVal::null(); }
	if ( sequence1.len == 0 || sequence2.len == 0 ) { return DoubleVal::null(); };

	std::size_t length = sequence1.len;
	if ( sequence2.len < sequence1.len ) {
		length = sequence2.len;
	}

	std::string seq1 ((const char *)sequence1.ptr,sequence1.len);
	std::string seq2 ((const char *)sequence2.ptr,sequence2.len);

	double pcd_distance = 0;
	unsigned int number_valid = 0;
	std::string buff = "";
	for (std::size_t i = 0; i < length; i++) {
		seq1[i] = toupper(seq1[i]);
		seq2[i] = toupper(seq2[i]);
		buff = std::string() + seq1[i] + seq2[i];
		if ( pcd.count( buff ) > 0 ) {
			pcd_distance += pcd[buff];
			number_valid++;
		}
	}

	if ( number_valid > 0 ) {
		pcd_distance /= (double) number_valid;
		return DoubleVal(pcd_distance);
	} else {
		return DoubleVal::null();
	}
}

IMPALA_UDF_EXPORT
StringVal Physiochemical_Distance_List(FunctionContext* context, const StringVal& sequence1, const StringVal& sequence2 ) {
	if ( sequence1.is_null  || sequence2.is_null  ) { return StringVal::null(); }
	if ( sequence1.len == 0 || sequence2.len == 0 ) { return StringVal::null(); };

	std::size_t length = sequence1.len;
	if ( sequence2.len < sequence1.len ) {
		length = sequence2.len;
	}

	std::string seq1 ((const char *)sequence1.ptr,sequence1.len);
	std::string seq2 ((const char *)sequence2.ptr,sequence2.len);

	double pcd_distance = 0;
	std::string buff = "";
	std::string result = "";

	std::size_t i =0;
	seq1[i] = toupper(seq1[i]); seq2[i] = toupper(seq2[i]);
	buff = std::string() + seq1[i] + seq2[i];
	if ( pcd.count( buff ) > 0 ) {
		result += std::to_string(pcd[buff]);
	} else {
		result += "NA";
	}

	for (i = 1; i < length; i++) {
		seq1[i] = toupper(seq1[i]);
		seq2[i] = toupper(seq2[i]);
		buff = std::string() + seq1[i] + seq2[i];
		if ( pcd.count( buff ) > 0 ) {
			result += " " + std::to_string(pcd[buff]);
		} else {
			result += " NA";
		}
	}

	return to_StringVal(context, result);
}

IMPALA_UDF_EXPORT
BooleanVal Contains_An_Element(FunctionContext* context, const StringVal& string1, const StringVal& string2, const StringVal& delimVal ) {
	if ( string1.is_null || string2.is_null || delimVal.is_null ) { return BooleanVal::null(); }
	if ( string1.len == 0 || string2.len == 0 ) { return BooleanVal(false); }

	std::string s1 ((const char *)string1.ptr,string1.len);
	std::string s2 ((const char *)string2.ptr,string2.len);
	std::string delim ((const char *)delimVal.ptr,delimVal.len);
	std::vector<std::string> tokens = split_by_substr(s2,delim);

	// if the delim = string, then of course nothing can be found
	if ( tokens.size() == 0 ) { return BooleanVal(false); }

	// otherwise search for the element
	for ( std::vector<std::string>::const_iterator i = tokens.begin(); i < tokens.end(); ++i) {
		if ( s1.find(*i) != std::string::npos && (*i).length() > 0 ) {
			return BooleanVal(true);
		}
	}

	// otherwise element was never found
	return BooleanVal(false);
}

IMPALA_UDF_EXPORT
BooleanVal Is_An_Element(FunctionContext* context, const StringVal& string1, const StringVal& string2, const StringVal& delimVal ) {
	if ( string1.is_null || string2.is_null || delimVal.is_null ) { return BooleanVal::null(); }
	if ( string1.len == 0 || string2.len == 0 ) { return BooleanVal(false); }

	std::string s1 ((const char *)string1.ptr,string1.len);
	std::string s2 ((const char *)string2.ptr,string2.len);
	std::string delim ((const char *)delimVal.ptr,delimVal.len);
	std::vector<std::string> tokens = split_by_substr(s2,delim);

	if ( tokens.size() == 0 ) { return BooleanVal(false); }
	for ( std::vector<std::string>::const_iterator i = tokens.begin(); i < tokens.end(); ++i) {
		if ( s1 == (*i) && (*i).length() > 0 ) {
			return BooleanVal(true);
		}
	}
	return BooleanVal(false);
}

IMPALA_UDF_EXPORT
BooleanVal Contains_Symmetric(FunctionContext* context, const StringVal& string1, const StringVal& string2 ) {
	if ( string1.is_null || string2.is_null  ) { return BooleanVal::null(); }
	if ( (string1.len == 0) != (string2.len == 0) ) { return BooleanVal(false); }

	std::string s1 ((const char *)string1.ptr,string1.len);
	std::string s2 ((const char *)string2.ptr,string2.len);

	if ( s1.find(s2) != std::string::npos || s2.find(s1) != std::string::npos ) {
		return BooleanVal(true);
	} else {
		return BooleanVal(false);
	}
}

IMPALA_UDF_EXPORT
StringVal nt_id(FunctionContext* context, const StringVal& sequence ) {
	if ( sequence.is_null  || sequence.len == 0  ) { return StringVal::null(); }
	std::string seq ((const char *)sequence.ptr,sequence.len);
	boost::remove_erase_if(seq, boost::is_any_of("\n\r\t :.~-"));
	boost::to_upper(seq);

	unsigned char obuf[21];
	SHA1( (const unsigned char*) seq.c_str(), seq.size(), obuf);
	
	char buffer[42 * sizeof(char)]; int j;
	for(j = 0; j < 20; j++) {
	    sprintf(&buffer[2*j*sizeof(char)], "%02x", obuf[j]);
	}

	return to_StringVal(context, buffer);
}

IMPALA_UDF_EXPORT
StringVal nt_std(FunctionContext* context, const StringVal& sequence ) {
	if ( sequence.is_null  || sequence.len == 0  ) { return StringVal::null(); }
	std::string seq ((const char *)sequence.ptr,sequence.len);
	boost::remove_erase_if(seq, boost::is_any_of("\n\r\t :.~-"));
	boost::to_upper(seq);

	return to_StringVal(context, seq);
}

IMPALA_UDF_EXPORT
StringVal aa_std(FunctionContext* context, const StringVal &sequence ) {
	if ( sequence.is_null  || sequence.len == 0  ) { return StringVal::null(); }
	std::string seq ((const char *)sequence.ptr,sequence.len);
	boost::remove_erase_if(seq, boost::is_any_of("\n\r\t :.-"));
	boost::to_upper(seq);

	return to_StringVal(context, seq);
}

IMPALA_UDF_EXPORT
StringVal variant_hash(FunctionContext* context, const StringVal &sequence ) {
	if ( sequence.is_null  || sequence.len == 0  ) { return StringVal::null(); }
	std::string seq ((const char *)sequence.ptr,sequence.len);
	boost::remove_erase_if(seq, boost::is_any_of("\n\r\t :.-"));
	boost::to_upper(seq);

	unsigned char obuf[17];
	MD5( (const unsigned char*) seq.c_str(), seq.size(), obuf);
	
	char buffer[34 * sizeof(char)]; int j;
	for(j = 0; j < 16; j++) {
	    sprintf(&buffer[2*j*sizeof(char)], "%02x", obuf[j]);
	}

	return to_StringVal(context, buffer);
}

IMPALA_UDF_EXPORT
StringVal md5(FunctionContext* context, int num_vars, const StringVal* args ) {
	if ( num_vars == 0 || args[0].is_null ) { return StringVal::null(); }
	std::string input ((const char *)args[0].ptr,args[0].len);
	std::string delim = "\a";
	for( int i = 1; i < num_vars; i++ ) {
		if ( args[i].is_null ) {
			return StringVal::null();
		} else if ( args[i].len == 0 ) {
			input += delim;
		} else {
			std::string next_var ((const char *)args[i].ptr,args[i].len);
			input += delim + next_var.c_str();
		}
	}
	if ( input.size() == 0 ) { return StringVal::null(); }

	unsigned char obuf[17];
	MD5( (const unsigned char*) input.c_str(), input.size(), obuf);
	
	char buffer[34 * sizeof(char)]; int j;
	for(j = 0; j < 16; j++) {
	    sprintf(&buffer[2*j*sizeof(char)], "%02x", obuf[j]);
	}

	return to_StringVal(context, buffer);
}

IMPALA_UDF_EXPORT
IntVal Number_Deletions(FunctionContext* context, const StringVal& sequence ) {
	if ( sequence.is_null )	  { return IntVal::null(); } 
	if ( sequence.len == 0 ) { return IntVal(0); }

	std::string seq ((const char *)sequence.ptr,sequence.len);
	int number_of_indels = 0;
	int open = 0;
	
	for( int i = 1; i < seq.size(); i++ ) {
		if ( seq[i] == '-' ) {
			if ( isalpha(seq[i-1]) ) {
				open = 1;
			}
		} else if ( isalpha(seq[i]) ) {
			if ( open > 0 ) {
				number_of_indels++;
				open = 0;
			}		
		}
	}	

	return IntVal(number_of_indels);
}

IMPALA_UDF_EXPORT
IntVal Longest_Deletion(FunctionContext* context, const StringVal& sequence ) {
	if ( sequence.is_null )	  { return IntVal::null(); } 
	if ( sequence.len == 0 ) { return IntVal(0); }

	std::string seq ((const char *)sequence.ptr,sequence.len);
	int longest_del = 0;		// Longest deletion length
	int open = 0;			// Open deletion length
	
	for( int i = 1; i < seq.size(); i++ ) {
		if ( seq[i] == '-' ) {
			if ( isalpha(seq[i-1]) ) {
				open = 1;
			} else if ( open > 0 ) {
				open++;
			}
		} else if ( isalpha(seq[i]) ) {
			if ( open > 0 ) {
				if ( open > longest_del ) {
					longest_del = open;
				}
				open = 0;
			}	
		}
	}	

	return IntVal(longest_del);
}
