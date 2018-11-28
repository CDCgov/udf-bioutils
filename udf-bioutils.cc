// udf-bioutils.cc - Sam Shepard - 2018
// Impala user-defined functions for CDC biofinformatics.
// Relies on Cloudera headers being installed.
// Current version supports C++98

#include "udf-sample.h"

#include <cctype>
#include <cmath>
#include <string>
#include <algorithm>
#include <vector>
#include <sstream>
#include <map>
#include <locale>
#include <boost/unordered_map.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/algorithm/string.hpp>

// Initialize hashes for C++98
boost::unordered_map<std::string,std::string> get_genetic_code() {
	boost::unordered_map<std::string,std::string> m;
        m["TAA"]="*";m["TAG"]="*";m["TAR"]="*";m["TGA"]="*";m["TRA"]="*";m["GCA"]="A";m["GCB"]="A";m["GCC"]="A";m["GCD"]="A";m["GCG"]="A";m["GCH"]="A";
        m["GCK"]="A";m["GCM"]="A";m["GCN"]="A";m["GCR"]="A";m["GCS"]="A";m["GCT"]="A";m["GCV"]="A";m["GCW"]="A";m["GCY"]="A";m["TGC"]="C";m["TGT"]="C";
        m["TGY"]="C";m["GAC"]="D";m["GAT"]="D";m["GAY"]="D";m["GAA"]="E";m["GAG"]="E";m["GAR"]="E";m["TTC"]="F";m["TTT"]="F";m["TTY"]="F";m["GGA"]="G";
        m["GGB"]="G";m["GGC"]="G";m["GGD"]="G";m["GGG"]="G";m["GGH"]="G";m["GGK"]="G";m["GGM"]="G";m["GGN"]="G";m["GGR"]="G";m["GGS"]="G";m["GGT"]="G";
        m["GGV"]="G";m["GGW"]="G";m["GGY"]="G";m["CAC"]="H";m["CAT"]="H";m["CAY"]="H";m["ATA"]="I";m["ATC"]="I";m["ATH"]="I";m["ATM"]="I";m["ATT"]="I";
        m["ATW"]="I";m["ATY"]="I";m["AAA"]="K";m["AAG"]="K";m["AAR"]="K";m["CTA"]="L";m["CTB"]="L";m["CTC"]="L";m["CTD"]="L";m["CTG"]="L";m["CTH"]="L";
        m["CTK"]="L";m["CTM"]="L";m["CTN"]="L";m["CTR"]="L";m["CTS"]="L";m["CTT"]="L";m["CTV"]="L";m["CTW"]="L";m["CTY"]="L";m["TTA"]="L";m["TTG"]="L";
        m["TTR"]="L";m["YTA"]="L";m["YTG"]="L";m["YTR"]="L";m["ATG"]="M";m["AAC"]="N";m["AAT"]="N";m["AAY"]="N";m["CCA"]="P";m["CCB"]="P";m["CCC"]="P";
        m["CCD"]="P";m["CCG"]="P";m["CCH"]="P";m["CCK"]="P";m["CCM"]="P";m["CCN"]="P";m["CCR"]="P";m["CCS"]="P";m["CCT"]="P";m["CCV"]="P";m["CCW"]="P";
        m["CCY"]="P";m["CAA"]="Q";m["CAG"]="Q";m["CAR"]="Q";m["AGA"]="R";m["AGG"]="R";m["AGR"]="R";m["CGA"]="R";m["CGB"]="R";m["CGC"]="R";m["CGD"]="R";
        m["CGG"]="R";m["CGH"]="R";m["CGK"]="R";m["CGM"]="R";m["CGN"]="R";m["CGR"]="R";m["CGS"]="R";m["CGT"]="R";m["CGV"]="R";m["CGW"]="R";m["CGY"]="R";
        m["MGA"]="R";m["MGG"]="R";m["MGR"]="R";m["AGC"]="S";m["AGT"]="S";m["AGY"]="S";m["TCA"]="S";m["TCB"]="S";m["TCC"]="S";m["TCD"]="S";m["TCG"]="S";
        m["TCH"]="S";m["TCK"]="S";m["TCM"]="S";m["TCN"]="S";m["TCR"]="S";m["TCS"]="S";m["TCT"]="S";m["TCV"]="S";m["TCW"]="S";m["TCY"]="S";m["ACA"]="T";
        m["ACB"]="T";m["ACC"]="T";m["ACD"]="T";m["ACG"]="T";m["ACH"]="T";m["ACK"]="T";m["ACM"]="T";m["ACN"]="T";m["ACR"]="T";m["ACS"]="T";m["ACT"]="T";
        m["ACV"]="T";m["ACW"]="T";m["ACY"]="T";m["GTA"]="V";m["GTB"]="V";m["GTC"]="V";m["GTD"]="V";m["GTG"]="V";m["GTH"]="V";m["GTK"]="V";m["GTM"]="V";
        m["GTN"]="V";m["GTR"]="V";m["GTS"]="V";m["GTT"]="V";m["GTV"]="V";m["GTW"]="V";m["GTY"]="V";m["TGG"]="W";m["TAC"]="Y";m["TAT"]="Y";m["TAY"]="Y";
	m["---"]="-";m["..."]=".";m["~~~"]="~";
	return m;
}

boost::unordered_map<std::string,int> get_ambig_equal() {
	boost::unordered_map<std::string,int> m;
	m["AM"]=1;m["MA"]=1;m["CM"]=1;m["MC"]=1;m["AV"]=1;m["VA"]=1;m["CV"]=1;m["VC"]=1;
	m["GV"]=1;m["VG"]=1;m["AN"]=1;m["NA"]=1;m["CN"]=1;m["NC"]=1;m["GN"]=1;m["NG"]=1;
	m["TN"]=1;m["NT"]=1;m["AH"]=1;m["HA"]=1;m["CH"]=1;m["HC"]=1;m["TH"]=1;m["HT"]=1;
	m["AR"]=1;m["RA"]=1;m["GR"]=1;m["RG"]=1;m["AD"]=1;m["DA"]=1;m["GD"]=1;m["DG"]=1;
	m["TD"]=1;m["DT"]=1;m["AW"]=1;m["WA"]=1;m["TW"]=1;m["WT"]=1;m["CS"]=1;m["SC"]=1;
	m["GS"]=1;m["SG"]=1;m["CB"]=1;m["BC"]=1;m["GB"]=1;m["BG"]=1;m["TB"]=1;m["BT"]=1;
	m["CY"]=1;m["YC"]=1;m["TY"]=1;m["YT"]=1;m["GK"]=1;m["KG"]=1;m["TK"]=1;m["KT"]=1;
	return m;
}

boost::unordered_map<std::string,std::string> gc = get_genetic_code();
boost::unordered_map<std::string,int> ambig_equal = get_ambig_equal();

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
