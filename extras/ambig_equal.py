#!/usr/bin/env python3


d = {}
d["A"] = set("A")
d["C"] = set("C")
d["G"] = set("G")
d["T"] = set("T")
d["R"] = set(["A","G"])
d["Y"] = set(["C","T"])
d["S"] = set(["G","C"])
d["W"] = set(["A","T"])
d["K"] = set(["G","T"])
d["M"] = set(["A","C"])
d["B"] = set(["C","G","T"])
d["D"] = set(["A","G","T"])
d["H"] = set(["A","C","T"])
d["V"] = set(["A","C","G"])
d["N"] = set(["A","G","C","T","U","R","Y","S","W","K","M","B","D","H","V"])

sorted_key = sorted(d, key = lambda k: len(d[k]) or k, reverse=True)

s1 = ""
s2 = ""
for v in sorted_key:
    for m in d:
        if v != m and d[m].issubset(d[v]):
            print(v,m)
            s1 += v
            s2 += m
            if "T" in d[m] and len(d[m]) == 1:
                print(v,"U")
                s1 += v
                s2 += "U"
print(len(s1),len(s2))

print(f'const char equal_base1[E+1]     = "{s1}";')
print(f'const char equal_base2[E+1]     = "{s2}";')
