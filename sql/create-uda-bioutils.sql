CREATE AGGREGATE FUNCTION IF NOT EXISTS udx.logfold_agreement(bigint)
    RETURNS double 
    INTERMEDIATE string 
    LOCATION "$UDA_BIOUTILS" 
    INIT_FN = "BoundedArrayInit" 
    UPDATE_FN = "BoundedArrayUpdate" 
    MERGE_FN = "BoundedArrayMerge" 
    SERIALIZE_FN = "StringStructSerialize" 
    FINALIZE_FN = "AgreementFinalize";

CREATE AGGREGATE FUNCTION IF NOT EXISTS udx.bitwise_sum(bigint) 
    RETURNS bigint 
    LOCATION "$UDA_BIOUTILS" 
    INIT_FN = "BitwiseOrInit" 
    UPDATE_FN = "BitwiseOrUpdateMerge" 
    MERGE_FN = "BitwiseOrUpdateMerge" 
    FINALIZE_FN = "BitwiseOrFinalize";

CREATE AGGREGATE FUNCTION IF NOT EXISTS udx.skewness(BIGINT)
    RETURNS DOUBLE
    INTERMEDIATE STRING
    LOCATION "$UDA_BIOUTILS"
    UPDATE_FN="RunningMomentUpdate"
    INIT_FN="RunningMomentInit"
    MERGE_FN="RunningMomentMerge"
    FINALIZE_FN="RunningMomentSkewnessFinalize";

CREATE AGGREGATE FUNCTION IF NOT EXISTS udx.skewness(DOUBLE)
    RETURNS DOUBLE
    INTERMEDIATE STRING
    LOCATION "$UDA_BIOUTILS"
    UPDATE_FN="RunningMomentUpdate"
    INIT_FN="RunningMomentInit"
    MERGE_FN="RunningMomentMerge"
    FINALIZE_FN="RunningMomentSkewnessFinalize";

CREATE AGGREGATE FUNCTION IF NOT EXISTS udx.kurtosis(BIGINT)
    RETURNS DOUBLE
    INTERMEDIATE STRING
    LOCATION "$UDA_BIOUTILS"
    UPDATE_FN="RunningMomentUpdate"
    INIT_FN="RunningMomentInit"
    MERGE_FN="RunningMomentMerge"
    FINALIZE_FN="RunningMomentKurtosisFinalize";

CREATE AGGREGATE FUNCTION IF NOT EXISTS udx.kurtosis(DOUBLE)
    RETURNS DOUBLE
    INTERMEDIATE STRING
    LOCATION "$UDA_BIOUTILS"
    UPDATE_FN="RunningMomentUpdate"
    INIT_FN="RunningMomentInit"
    MERGE_FN="RunningMomentMerge"
    FINALIZE_FN="RunningMomentKurtosisFinalize";

CREATE AGGREGATE FUNCTION IF NOT EXISTS udx.logfold_agreement(BIGINT)
    RETURNS DOUBLE
    INTERMEDIATE STRING
    LOCATION "$UDA_BIOUTILS"
    UPDATE_FN="BoundedArrayUpdate"
    INIT_FN="BoundedArrayInit"
    MERGE_FN="BoundedArrayMerge"
    FINALIZE_FN="AgreementFinalize";
