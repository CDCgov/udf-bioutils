CREATE AGGREGATE FUNCTION IF NOT EXISTS udx.logfold_agreement(bigint)
    RETURNS double 
    INTERMEDIATE string 
    LOCATION "$UDF_BIOUTILS_PATH/libudabioutils.so" 
    INIT_FN = "BoundedArrayInit" 
    UPDATE_FN = "BoundedArrayUpdate" 
    MERGE_FN = "BoundedArrayMerge" 
    SERIALIZE_FN = "StringStructSerialize" 
    FINALIZE_FN = "AgreementFinalize";

CREATE AGGREGATE FUNCTION IF NOT EXISTS udx.bitwise_sum(bigint) 
    RETURNS bigint 
    LOCATION "$UDF_BIOUTILS_PATH/libudabioutils.so" 
    INIT_FN = "BitwiseOrInit" 
    UPDATE_FN = "BitwiseOrUpdateMerge" 
    MERGE_FN = "BitwiseOrUpdateMerge" 
    FINALIZE_FN = "BitwiseOrFinalize";

CREATE AGGREGATE FUNCTION IF NOT EXISTS udx.skewness(BIGINT)
    RETURNS DOUBLE
    INTERMEDIATE STRING
    LOCATION "$UDF_BIOUTILS_PATH/libudabioutils.so"
    UPDATE_FN="RunningMomentUpdate"
    INIT_FN="RunningMomentInit"
    MERGE_FN="RunningMomentMerge"
    FINALIZE_FN="RunningMomentSkewnessFinalize";

CREATE AGGREGATE FUNCTION IF NOT EXISTS udx.skewness(DOUBLE)
    RETURNS DOUBLE
    INTERMEDIATE STRING
    LOCATION "$UDF_BIOUTILS_PATH/libudabioutils.so"
    UPDATE_FN="RunningMomentUpdate"
    INIT_FN="RunningMomentInit"
    MERGE_FN="RunningMomentMerge"
    FINALIZE_FN="RunningMomentSkewnessFinalize";

CREATE AGGREGATE FUNCTION IF NOT EXISTS udx.kurtosis(BIGINT)
    RETURNS DOUBLE
    INTERMEDIATE STRING
    LOCATION "$UDF_BIOUTILS_PATH/libudabioutils.so"
    UPDATE_FN="RunningMomentUpdate"
    INIT_FN="RunningMomentInit"
    MERGE_FN="RunningMomentMerge"
    FINALIZE_FN="RunningMomentKurtosisFinalize";

CREATE AGGREGATE FUNCTION IF NOT EXISTS udx.kurtosis(DOUBLE)
    RETURNS DOUBLE
    INTERMEDIATE STRING
    LOCATION "$UDF_BIOUTILS_PATH/libudabioutils.so"
    UPDATE_FN="RunningMomentUpdate"
    INIT_FN="RunningMomentInit"
    MERGE_FN="RunningMomentMerge"
    FINALIZE_FN="RunningMomentKurtosisFinalize";

CREATE AGGREGATE FUNCTION IF NOT EXISTS udx.logfold_agreement(BIGINT)
    RETURNS DOUBLE
    INTERMEDIATE STRING
    LOCATION "$UDF_BIOUTILS_PATH/libudabioutils.so"
    UPDATE_FN="BoundedArrayUpdate"
    INIT_FN="BoundedArrayInit"
    MERGE_FN="BoundedArrayMerge"
    FINALIZE_FN="AgreementFinalize";

CREATE AGGREGATE FUNCTION IF NOT EXISTS udx.alphanumeric_entropy(STRING)
    RETURNS DOUBLE
    INTERMEDIATE STRING
    LOCATION "$UDF_BIOUTILS_PATH/libudabioutils.so"
    INIT_FN="CalcCharEntropyInit"
    UPDATE_FN="CalcCharEntropyUpdate"
    MERGE_FN="CalcCharEntropyMerge"
    SERIALIZE_FN="CalcCharEntropySerialize"
    FINALIZE_FN="CalcCharEntropyFinalize";

CREATE AGGREGATE FUNCTION IF NOT EXISTS udx.nt_entropy(STRING)
    RETURNS DOUBLE
    INTERMEDIATE STRING
    LOCATION "$UDF_BIOUTILS_PATH/libudabioutils.so"
    INIT_FN="CalcNTEntropyInit"
    UPDATE_FN="CalcNTEntropyUpdate"
    MERGE_FN="CalcNTEntropyMerge"
    SERIALIZE_FN="CalcNTEntropySerialize"
    FINALIZE_FN="CalcNTEntropyFinalize";

CREATE AGGREGATE FUNCTION IF NOT EXISTS udx.aa_entropy(STRING)
    RETURNS DOUBLE
    INTERMEDIATE STRING
    LOCATION "$UDF_BIOUTILS_PATH/libudabioutils.so"
    INIT_FN="CalcAAEntropyInit"
    UPDATE_FN="CalcAAEntropyUpdate"
    MERGE_FN="CalcAAEntropyMerge"
    SERIALIZE_FN="CalcAAEntropySerialize"
    FINALIZE_FN="CalcAAEntropyFinalize";

CREATE AGGREGATE FUNCTION IF NOT EXISTS udx.codon_entropy(STRING)
    RETURNS DOUBLE
    INTERMEDIATE STRING
    LOCATION "$UDF_BIOUTILS_PATH/libudabioutils.so"
    INIT_FN="CalcCDEntropyInit"
    UPDATE_FN="CalcCDEntropyUpdate"
    MERGE_FN="CalcCDEntropyMerge"
    SERIALIZE_FN="CalcCDEntropySerialize"
    FINALIZE_FN="CalcCDEntropyFinalize";
