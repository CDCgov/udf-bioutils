// udf-sero.cc - 2019
// UDFs for Sero. Definite overkill.
// Relies on Cloudera headers being installed.
// Current version supports C++11

#include "udf-sero.h"
#include "common.h"

#include <cctype>
#include <string>
#include <algorithm>
#include <vector>
#include <iostream>
#include <locale>

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

inline long get_number(const std::string& s) {
	std::size_t const pos = s.find_first_of("0123456789");
	if (pos != std::string::npos) {
		std::size_t const not_digit = s.find_first_not_of("0123456789", pos);
		// if non-digit found, length = pos - not_digit
		// else, until the end of the string
		return std::stol(s.substr(pos, not_digit != std::string::npos ? not_digit - pos : std::string::npos));
	} else {
		return 0;
	}
}	

inline StringVal to_StringVal(FunctionContext* context, const std::string& s) {
	StringVal result(context, s.size());
	memcpy(result.ptr, s.c_str(), s.size());
	return result;
}

bool compare_cohorts(std::string a, std::string b) {
	long score_a;
	long score_b;

	std::transform(a.begin(), a.end(), a.begin(), tolower); 	
	std::transform(b.begin(), b.end(), b.begin(), tolower); 	

	if ( a.find("ped") != std::string::npos ) { 
		score_a  = 0; 
	} else if ( a.find("adu") != std::string::npos ) { 
		score_a = 3; 
	} else if ( a.find("eld") != std::string::npos ) {
		score_a = 5;
	} else {
		score_a = 7;
	}

	if ( a.find("old") != std::string::npos ) { 
		score_a += 1; 
	} else if ( a.find("young") != std::string::npos || a.find("mo") != std::string::npos ) { 
		score_a -= 1; 
	}

	if ( b.find("ped") != std::string::npos ) { 
		score_b  = 0; 
	} else if ( b.find("adu") != std::string::npos ) { 
		score_b = 3; 
	} else if ( b.find("eld") != std::string::npos ) {
		score_b = 5;
	} else {
		score_b = 7;
	}

	if ( b.find("old") != std::string::npos ) { 
		score_b += 1; 
	} else if ( b.find("young") != std::string::npos || b.find("mo") != std::string::npos ) { 
		score_b -= 1; 
	}

	if ( score_a == 7 && score_b != 7 ) {
		return false;
	} else if ( score_a != 7 && score_b == 7 ) {
		return true;
	} else if ( score_a == 7 && score_b == 7 ) {
		return a.compare(b) < 0;
	} else if ( score_a == score_b ) {
		score_a = get_number(a);
		score_b = get_number(b);
		return score_a < score_b;
	} else {
		return score_a < score_b;
	}
}

IMPALA_UDF_EXPORT
StringVal Sort_Cohorts(FunctionContext* context, const StringVal& listVal, const StringVal& delimVal ) {
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
		std::sort(tokens.begin(), tokens.end(), compare_cohorts);
		std::vector<std::string> unique;
	
		unique.push_back(tokens[0]);
		for ( std::size_t i = 1; i < tokens.size(); i++) {
			if ( tokens[i] != tokens[i-1] ) {
				unique.push_back(tokens[i]);
			}
		}

		std::string s = unique[0];
		for ( std::size_t i = 1; i < unique.size(); i++) {
			if ( i == (unique.size()-1) ) {
				s += " and " + unique[i];
			} else {
				s += ", " + unique[i];
			}
		}
		
		return to_StringVal(context, s);
	}
}
