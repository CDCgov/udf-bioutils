# Requirements

This library assumes a GCC 9+ toolchain, [cmake](https://cmake.org), [Impala](https://impala.apache.org) UDF headers, and relevant [boost](https://www.boost.org) & openssl libraries are installed in the development environment. We have tested it on RHEL 8. Runtime dependencies such as OpenSSL need to be installed on the target Impala daemons. The `CMakeLists.txt` file may specify the minimum cpu target for optimization reasons. By default it is set to [Cascade Lake](https://en.wikipedia.org/wiki/Cascade_Lake).

## Install Impala Development Package

Cloudera provides [a development library](https://docs.cloudera.com/cdp-private-cloud-base/latest/impala-sql-reference/topics/impala-udf-installing-the-udf-development-package.html#udf_demo_env) for creating UDFs and UDAs. It is best to find a package version corresponding to targeted version of Impala (or older). You can reach out to Cloudera Support if you have difficulties getting the correct RPM. The package is likely to be called either `impala-udf-devel` or `impala-udf-devel-client`. As an example, one could use the following for CDP 7.1.9 runtime:

```bash
# Set Cloudera username and password according to license info.
wget https://$username:$password@archive.cloudera.com/p/cdh7/7.1.9.0/impala/redhat8/impala-udf-devel-client-4.0.0.7.1.9.0-387.x86_64.rpm
sudo rpm -i impala-udf-devel-client-4.0.0.7.1.9.0-387.x86_64.rpm
sudo yum info impala-udf-devel-client
```

Notice that Impala 4.0 is shipped with CDP 7.1.9 and the package indicates both the Impala and CDP version numbers in its name.

## Install Compiler Toolchain and Dependencies

On RHEL8, one can use so-called GCC toolsets to get a newer version of GCC that has binary compatibility with the base systems you are using. GCC 9 is required to get C++20 features used by this library. Later versions of GCC work just as well.

```bash
# Could gcc-toolset 9+, eg, gcc 9 to 14 on RHEL 8. Pick the toolchain based on your needs.
sudo yum install gcc-toolset-12 boost cmake openssl openssl-devel
```

We use OpenSSL for hashing functions, but Cloudera Manager Agent will also require OpenSSL, so that dependency will usually be met in terms of the the target daemons.

## Compilation

Configuration is done via `cmake` but `make` handles the build process. UDX tests are also provided. The test artifacts should be able to execute on the target Impala daemon. If they do not additional dependencies may be missing.

```bash
# Load the toolchain.
# This example assumes `gcc-toolset-12`` was installed on RHEL 8.
source /opt/rh/gcc-toolset-12/enable

# Configure and compile
cmake .
make

## Run tests
./build/udf-bioutils-test
./build/uda-bioutils-test
```

Built libraries will need to be put in HDFS / S3 / ADLS and then instantiated within Impala using the appropriate SQL.

## Deployment

Deployment requires pushing our `.so` files to the remote, distributed file system and then registering the functions with Impala via query. If we have already deployed and registered our functions and just want to update them, we can just push to the remote storage system and use Impala's `REFRESH FUNCTION` [syntax](https://impala.apache.org/docs/build/html/topics/impala_refresh_functions.html).

### Set Vars

Suppose we have 3 install locations for the included libraries. We can set some environmental variable like so:

```bash
# Set HDFS (or other store) locations for the libraries

export UDF_MATHUTILS="/udx/ncird_id/prod/libudfmathutils.so"
export UDF_BIOUTILS="/udx/ncird_id/prod/libudfbioutils.so"
export UDA_BIOUTILS="/udx/ncird_id/prod/libudabioutils.so"
```

We used a folder called `/udx` for both UDF and UDAs, a prod vs. dev subfolder, and a group for our organization which is NCIRD/ID. In practice you should set this to where you expect the UDFs to be readable by Impala. If they are not readable, adjust permissions and/or [Ranger](https://docs.cloudera.com/cdp-reference-architectures/latest/cdp-ra-security/topics/cdp-ra-security-apache-ranger.html) policies until they are.

### Push to the Remote

The library artifacts are in the build directory and we can use the Hadoop CLI to push them to our remote file system (in combination with our environment vars). See:

```bash
for libpath in build/*.so;do 
    lib=$(basename $libpath)
    remote_path="none"
    for path in "$UDF_MATHUTILS" "$UDF_BIOUTILS" "$UDA_BIOUTILS";do
        if [ "$(basename "$path")" == "$lib" ];then
            remote_path="$path"
            break
        fi
    done
    if [ "$remote_path" != "none" ];then
        echo "Pushing '$libpath' to '$remote_path'"
        hdfs dfs -put -f "$libpath" "$remote_path"
    fi
done
```

### Generate and run SQL

Next we can generate SQL to register the functions using our above variables:

```bash
# Creates tmp files to execute in Impala
for i in sql/*;do 
    cat $i|envsubst > $(basename $i .sql).tmp.sql
done
```

The query files can be executed via CLI or in something like HUE.
