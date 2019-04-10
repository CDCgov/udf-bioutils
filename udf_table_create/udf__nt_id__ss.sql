create table if not exists udx.udf__nt_id__ss (arg1_nucleotides string, outcome string);
insert overwrite udx.udf__nt_id__ss (arg1_nucleotides) values (""),(NULL),("ATGAACACTCAAATCCTGGTATTCGCTCTGGTGGCGAGCATTCCGACAAATGCA"),("ATGAACACTCAAATCCTGGTATTCGCTCTGGTGGCGAGCATTCCGACAAATGCA...   ---~~~:::"),("atgaacactcaaatcctggtattcgctctggtggcgagcattccgacaaatgca"),("ATGAACACTCAAATCCTGGTATTCGCTCTGGTGGCGAGCATTCCGACAAATGCg"),("1"),("TCC ACC GCC CGG AAA");
insert overwrite udx.udf__nt_id__ss select arg1_nucleotides,udx.nt_id(arg1_nucleotides) from udx.udf__nt_id__ss;
