// udf-bioutils.cc - Sam Shepard - 2018
// Impala user-defined functions for CDC biofinformatics.
// Relies on Cloudera headers being installed.
// Current version supports C++98

#include "udf-bioutils.h"

#include <cctype>
#include <cmath>
#include <string>
#include <algorithm>
#include <vector>
#include <sstream>
#include <locale>
#include <boost/unordered_map.hpp>
#include <boost/unordered_set.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/range/algorithm_ext/erase.hpp>
#include <openssl/sha.h>
#include <openssl/md5.h>

boost::unordered_map<std::string,std::string> gc = {
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

boost::unordered_set<std::string> ambig_equal = {
        "AM","MA","CM","MC","AV","VA","CV","VC","GV","VG","AN","NA","CN","NC","GN","NG",
        "TN","NT","AH","HA","CH","HC","TH","HT","AR","RA","GR","RG","AD","DA","GD","DG",
        "TD","DT","AW","WA","TW","WT","CS","SC","GS","SG","CB","BC","GB","BG","TB","BT",
        "CY","YC","TY","YT","GK","KG","TK","KT"
};

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
	StringVal result(context, s.size());
	memcpy(result.ptr, s.c_str(), s.size());
	return result;
}

// We take a string of delimited values in a string and sort it in ascending order
StringVal Sort_List_By_Substring(FunctionContext* context, const StringVal& listVal, const StringVal& delimVal ) {
	if ( listVal.is_null || delimVal.is_null ) { return StringVal::null(); }
	if ( listVal.len == 0 || delimVal.len == 0 ) { return listVal; };
	
	std::string list  ((const char *)listVal.ptr,listVal.len);
	std::string delim ((const char *)delimVal.ptr,delimVal.len);
	std::vector<std::string> tokens = split_by_substr(list,delim);

	// Use the usual ascending sort
	std::sort(tokens.begin(), tokens.end());
	std::string s = tokens[0];
	for ( std::vector<std::string>::const_iterator i = tokens.begin() +1; i < tokens.end(); ++i) { s += delim + *i; }

	return to_StringVal(context, s);
}

// We take a string of delimited values in a string and sort it in ascending order
StringVal Sort_List_By_Substring_Unique(FunctionContext* context, const StringVal& listVal, const StringVal& delimVal ) {
	if ( listVal.is_null || delimVal.is_null ) { return StringVal::null(); }
	if ( listVal.len == 0 || delimVal.len == 0 ) { return listVal; };
	
	std::string list  ((const char *)listVal.ptr,listVal.len);
	std::string delim ((const char *)delimVal.ptr,delimVal.len);
	std::vector<std::string> tokens = split_by_substr(list,delim);

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
StringVal Sort_Allele_List(FunctionContext* context, const StringVal& listVal, const StringVal& delimVal ) {
	if ( listVal.is_null || delimVal.is_null ) { return StringVal::null(); }
	if ( listVal.len == 0 || delimVal.len == 0 ) { return listVal; };

	std::string list  ((const char *)listVal.ptr,listVal.len);
	std::string delim ((const char *)delimVal.ptr,delimVal.len);
	std::vector<std::string> tokens = split_by_substr(list,delim);

	// Use the usual ascending sort
	std::sort(tokens.begin(), tokens.end(), comp_allele);
	std::string s = tokens[0];
	for ( std::vector<std::string>::const_iterator i = tokens.begin() +1; i < tokens.end(); ++i) { s += delim + *i; }

	return to_StringVal(context,s);
}

// We take codon(s) and translate it/them
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
//			std::string::size_type lastPos = tokens[i].find_first_not_of("..", 0);
//			std::string::size_type pos     = tokens[i].find_first_of("..", lastPos);
//			while (std::string::npos != pos || std::string::npos != lastPos) {
//				range.push_back(tokens[i].substr(lastPos, pos - lastPos));
//				lastPos = tokens[i].find_first_not_of("..", pos);
//				pos = tokens[i].find_first_of("..", lastPos);
//			}
		
			a = atoi(range[0].c_str()) - 1;
			b = atoi(range[1].c_str()) - 1;

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
			x = atoi(tokens[i].c_str()) - 1;
			if ( x < L && x >= 0 ) {
				buffer += seq[x];
			}
		}
	}

	return to_StringVal(context,buffer);
}

// Create a mutation list from two aligned strings
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
// Ignore resolvable ambiguations
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

IntVal Hamming_Distance_Pairwise_Delete(FunctionContext* context, const StringVal& sequence1, const StringVal& sequence2, const StringVal& pairwise_delete_set ) {
	if ( sequence1.is_null  || sequence2.is_null || pairwise_delete_set.is_null  ) { return IntVal::null(); }
	if ( sequence1.len == 0 || sequence2.len == 0 ) { return IntVal::null(); };

	std::size_t length = sequence1.len;
	if ( sequence2.len < sequence1.len ) {
		length = sequence2.len;
	}

	std::string seq1 ((const char *)sequence1.ptr,sequence1.len);
	std::string seq2 ((const char *)sequence2.ptr,sequence2.len);
	boost::unordered_map<char,int> m;

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

BooleanVal Contains_An_Element(FunctionContext* context, const StringVal& string1, const StringVal& string2, const StringVal& delimVal ) {
	if ( string1.is_null || string2.is_null || delimVal.is_null ) { return BooleanVal::null(); }
	if ( string1.len == 0 || string2.len == 0 ) { return BooleanVal(false); }

	std::string s1 ((const char *)string1.ptr,string1.len);
	std::string s2 ((const char *)string2.ptr,string2.len);
	std::string delim ((const char *)delimVal.ptr,delimVal.len);
	std::vector<std::string> tokens = split_by_substr(s2,delim);

	for ( std::vector<std::string>::const_iterator i = tokens.begin(); i < tokens.end(); ++i) {
		if ( s1.find(*i) != std::string::npos && (*i).length() > 0 ) {
			return BooleanVal(true);
		}
	}
	return BooleanVal(false);
}

BooleanVal Is_An_Element(FunctionContext* context, const StringVal& string1, const StringVal& string2, const StringVal& delimVal ) {
	if ( string1.is_null || string2.is_null || delimVal.is_null ) { return BooleanVal::null(); }
	if ( string1.len == 0 || string2.len == 0 ) { return BooleanVal(false); }

	std::string s1 ((const char *)string1.ptr,string1.len);
	std::string s2 ((const char *)string2.ptr,string2.len);
	std::string delim ((const char *)delimVal.ptr,delimVal.len);
	std::vector<std::string> tokens = split_by_substr(s2,delim);

	for ( std::vector<std::string>::const_iterator i = tokens.begin(); i < tokens.end(); ++i) {
		if ( s1 == (*i) && (*i).length() > 0 ) {
			return BooleanVal(true);
		}
	}
	return BooleanVal(false);
}

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

StringVal nt_id(FunctionContext* context, const StringVal& sequence ) {
	if ( sequence.is_null  || sequence.len == 0  ) { return StringVal::null(); }
	std::string seq ((const char *)sequence.ptr,sequence.len);
	boost::remove_erase_if(seq, boost::is_any_of(" :.~-"));
	boost::to_upper(seq);

	unsigned char obuf[21];
	SHA1( (const unsigned char*) seq.c_str(), seq.size(), obuf);
	
	char buffer[42 * sizeof(char)]; int j;
	for(j = 0; j < 20; j++) {
	    sprintf(&buffer[2*j*sizeof(char)], "%02x", obuf[j]);
	}

	return to_StringVal(context, buffer);
}

StringVal variant_hash(FunctionContext* context, const StringVal &sequence ) {
	if ( sequence.is_null  || sequence.len == 0  ) { return StringVal::null(); }
	std::string seq ((const char *)sequence.ptr,sequence.len);
	boost::remove_erase_if(seq, boost::is_any_of(" :.-"));
	boost::to_upper(seq);

	unsigned char obuf[17];
	MD5( (const unsigned char*) seq.c_str(), seq.size(), obuf);
	
	char buffer[34 * sizeof(char)]; int j;
	for(j = 0; j < 16; j++) {
	    sprintf(&buffer[2*j*sizeof(char)], "%02x", obuf[j]);
	}

	return to_StringVal(context, buffer);
}
