// Samuel S. Shepard, CDC

#include <charconv>
#include <map>
#include <set>
#include <string>
#include <unordered_set>
#include <vector>

#include <boost/spirit/include/karma.hpp>
#include <impala_udf/udf.h>

// SPLIT STRING/VIEW by substring (uses search)
// Modified from: https://github.com/fenbf/StringViewTests/blob/master/StringViewTest.cpp
// The lifeime of the input 'str' must be greater than or equal to the lifetime of elements in the
// output
inline std::vector<std::string_view> split_by_substr(
    std::string_view str, std::string_view delim_str
) {
    const std::size_t LD = delim_str.size();
    std::vector<std::string_view> output;
    for (auto first = str.data(), second = str.data(), last = first + str.size();
         second != last && first != last; first = second + LD) {

        second = std::search(first, last, std::cbegin(delim_str), std::cend(delim_str));

        if (first != second) {
            output.emplace_back(first, second - first);
        }
    }

    return output;
}

inline BooleanVal character_in_string(const StringVal &needle, const StringVal &list_of_items) {
    if (needle.len != 1) {
        return BooleanVal(false);
    }

    const char query = *needle.ptr;
    std::string items((const char *)list_of_items.ptr, list_of_items.len);
    return BooleanVal(items.find(query) != std::string::npos);
}

inline std::vector<std::string> split_by_substr(
    const std::string &str, const std::string &delim_str
) {
    const std::size_t LD = delim_str.size();
    std::vector<std::string> output;
    for (auto first = str.data(), second = str.data(), last = first + str.size();
         second != last && first != last; first = second + LD) {

        second = std::search(first, last, std::cbegin(delim_str), std::cend(delim_str));

        if (first != second) {
            output.emplace_back(first, second - first);
        }
    }

    return output;
}

// SPLIT STRING/VIEW by string of single delimiters (uses find_first_of)
// Courtesy: https://github.com/fenbf/StringViewTests/blob/master/StringViewTest.cpp
// The lifeime of the input 'str' must be greater than or equal to the lifetime of elements in the
// output
inline auto split_by_delims(std::string_view str, std::string_view delims) {
    std::vector<std::string_view> output;
    for (auto first = str.data(), second = str.data(), last = first + str.size();
         second != last && first != last; first = second + 1) {

        second = std::find_first_of(first, last, std::cbegin(delims), std::cend(delims));

        if (first != second) {
            output.emplace_back(first, second - first);
        }
    }

    return output;
}

// SPLIT STRING/VIEW by substring (uses search)
// Modified from:
// https://stackoverflow.com/questions/56634507/safely-convert-stdstring-view-to-int-like-stoi-or-atoi
inline std::vector<int> split_int_by_substr(std::string_view str, std::string_view delims) {
    const std::size_t ND = delims.size();
    std::vector<int> output;

    for (auto first = str.data(), second = str.data(), last = first + str.size();
         second != last && first != last; first = second + ND) {

        second = std::search(first, last, std::cbegin(delims), std::cend(delims));

        if (first != second) {
            int ivalue;
            auto conversion_result = std::from_chars(first, first + (second - first), ivalue);
            if (conversion_result.ec != std::errc::invalid_argument) {
                output.emplace_back(ivalue);
            } else {
                output.clear();
                return output;
            }
        }
    }

    return output;
}

// Legacy function to create an unordered set of integers from a delimited input string
inline std::vector<int> split_set_by_substr(const std::string &str, const std::string &delim) {
    std::unordered_set<std::string> tokens;
    std::size_t prev = 0;
    std::size_t pos  = 0;

    if (delim.length() == 0) {
        for (std::size_t k = 0; k < str.length(); k++) {
            tokens.insert(str.substr(k, 1));
        }
    } else {
        do {
            pos = str.find(delim, prev);
            if (pos == std::string::npos) {
                pos = str.length();
            }

            std::string token = str.substr(prev, pos - prev);
            if (!token.empty()) {
                tokens.insert(token);
            }
            prev = pos + delim.length();
        } while (pos < str.length() && prev < str.length());
    }

    std::vector<int> v;
    int num;
    for (auto it = tokens.begin(); it != tokens.end(); ++it) {
        try {
            num = std::stoi(*it);
        } catch (...) {
            continue;
        }
        v.push_back(num);
    }
    return v;
}

// Returns an ordered set (unique elements) of ints as a vector
// Faster than unordered for this purpose
inline std::vector<int> split_unique_sequence_by_substr(
    std::string_view str, std::string_view delims
) {
    const std::size_t ND = delims.size();
    std::vector<int> output;
    std::set<int> iset;

    for (auto first = str.data(), second = str.data(), last = first + str.size();
         second != last && first != last; first = second + ND) {

        second = std::search(first, last, std::cbegin(delims), std::cend(delims));

        if (first != second) {
            int ivalue;
            auto conversion_result = std::from_chars(first, first + (second - first), ivalue);
            if (conversion_result.ec != std::errc::invalid_argument) {
                iset.insert(ivalue);
            } else {
                return output;
            }
        }
    }

    for (auto const &i : iset) {
        output.emplace_back(i);
    }

    return output;
}

// Returns an ordered set (unique elements) of ints
// Faster than unordered for this purpose
inline std::set<int> split_ordered_set_by_substr(std::string_view str, std::string_view delims) {
    const std::size_t ND = delims.size();
    std::set<int> iset;

    for (auto first = str.data(), second = str.data(), last = first + str.size();
         second != last && first != last; first = second + ND) {

        second = std::search(first, last, std::cbegin(delims), std::cend(delims));

        if (first != second) {
            int ivalue;
            auto conversion_result = std::from_chars(first, first + (second - first), ivalue);
            if (conversion_result.ec != std::errc::invalid_argument) {
                iset.insert(ivalue);
            } else {
                iset.clear();
                return iset;
            }
        }
    }

    return iset;
}

// Returns an ordered map of string_views
inline std::map<std::string_view, int> split_ordered_map_by_substr(
    std::string_view str, std::string_view delims
) {
    const std::size_t ND = delims.size();
    std::map<std::string_view, int> svmap;

    for (auto first = str.data(), second = str.data(), last = first + str.size();
         second != last && first != last; first = second + ND) {

        second = std::search(first, last, std::cbegin(delims), std::cend(delims));

        if (first != second) {
            std::string_view sv(first, second - first);
            svmap[sv]++;
        }
    }

    return svmap;
}

// Copies a std::string into a StringVal for Impala memory management
// Is not preferred if you can only allocate the StringVal to begin with and operate on its pointer
inline StringVal to_StringVal(FunctionContext *context, const std::string &s) {
    if (s.size() > StringVal::MAX_LENGTH) {
        return StringVal::null();
    } else {
        StringVal result(context, s.size());
        memcpy(result.ptr, s.c_str(), s.size());
        return result;
    }
}

// Fast integer generation from a string
// TO-DO: Should be compared with C++20 options
inline bool append_int(std::string &s, std::size_t val) {
    return boost::spirit::karma::generate(std::back_inserter(s), val);
}
