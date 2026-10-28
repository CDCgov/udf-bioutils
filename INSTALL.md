# Requirements

This library assumes a GCC 9+ toolchain, [cmake](https://cmake.org), [Impala](https://impala.apache.org) UDF headers, and relevant [boost](https://www.boost.org) & openssl libraries are installed in the development environment. We have tested it on RHEL 8. Runtime dependencies such as OpenSSL need to be installed on the target Impala daemons. The `CMakeLists.txt` file may specify the minimum cpu target for optimization reasons. By default it is set to [Cascade Lake](https://en.wikipedia.org/wiki/Cascade_Lake).

## Install Impala Development Package

Cloudera provides [a development library](https://docs.cloudera.com/cdp-private-cloud-base/latest/impala-sql-reference/topics/impala-udf-installing-the-udf-development-package.html#udf_demo_env) for creating UDFs and UDAs. It is best to find a package version corresponding to targeted version of Impala (or older). You can reach out to Cloudera Support if you have difficulties getting the correct RPM. The package is likely to be called either `impala-udf-devel` or `impala-udf-devel-client`. As an example, one could use the following for CDP 7.1.9 runtime:

```bash
# Set Cloudera username and password according to license info.
wget https://$username:$password@archive.cloudera.com/p/cdh7/7.1.9.1080/impala/redhat8/impala-udf-devel-client-4.0.0.7.1.9.1080-4.x86_64.rpm
sudo rpm -i impala-udf-devel-client-4.0.0.7.1.9.0-387.x86_64.rpm
sudo yum info impala-udf-devel-client
```

Notice that Impala 4.0 is shipped with CDP 7.1.9 and the package indicates both the Impala and CDP version numbers in its name.

## Install Compiler Toolchain and Dependencies

On RHEL8, one can use so-called GCC toolsets to get a newer version of GCC that has binary compatibility with the base systems you are using. GCC 9 is required to get C++20 features used by this library. Later versions of GCC work just as well.

```bash
# Could be gcc-toolset 9+, eg, gcc 9 to 13 on RHEL 8 (GCC 13 tested). Pick the toolchain based on your needs.
sudo yum install gcc-toolset-13 boost cmake openssl openssl-devel
```

We use OpenSSL for hashing functions, but Cloudera Manager Agent will also require OpenSSL, so that dependency will usually be met in terms of the the target daemons.

For benchmarks, please install the [google-benchmark](https://github.com/google/benchmark) library:

```bash
sudo yum install -y google-benchmark-devel google-benchmark
```

## Compilation

Run from the repository root using CMake 3.20+:

```bash
# On RHEL 8, load the installed compiler toolchain.
source /opt/rh/gcc-toolset-13/enable

cmake --preset release
cmake --build --preset release
ctest --preset release
```

The preset builds Release with four parallel jobs and skips benchmarks. Libraries and
test executables are written to `build/`. CTest runs three C++ suites; no Impala server
is required. Rebuild before rerunning tests.

For an unoptimized build with debug symbols, use the `debug` preset for all three
commands. Its output goes to `build/debug/`. Override the job count with
`cmake --build --preset release --parallel N`.

Without presets, the build type defaults to Release. Explicit build types are preserved;
build parallelism is selected with `--parallel 4` or `CMAKE_BUILD_PARALLEL_LEVEL`.

| Configure option           | Purpose                                                                                                                   |
| -------------------------- | ------------------------------------------------------------------------------------------------------------------------- |
| `-DMARCH=x86-64`           | Select a baseline CPU target instead of the default `cascadelake`; choose a target supported by the destination machines. |
| `-DEXCLUDE_BENCHMARK=ON`   | Skip benchmarks and their dependencies; benchmarks are enabled by default.                                                |
| `-DCMAKE_BUILD_TYPE=Debug` | Build without optimization and with debug symbols.                                                                        |

For custom Boost locations, set both `BOOST_INCLUDEDIR` and `BOOST_LIBRARYDIR`
**environment variables** before configuring. Rerun configuration to change options;
use a fresh build directory when changing compilers or generators.

To build or run a subset:

```bash
cmake --build build --target uda-bioutils-test
ctest --test-dir build --output-on-failure -R '^uda-'
```

For benchmarks, install Google Benchmark and ensure `/bin/python` provides Python 3:

```bash
cmake -S . -B build -DEXCLUDE_BENCHMARK=OFF
cmake --build build --target udf-bioutils-bmark --parallel 4
./build/udf-bioutils-bmark --benchmark_filter='^BM_Hamming_Distance$'
```

See [benchmark/BENCHMARK.md](benchmark/BENCHMARK.md) for benchmark inputs. Existing
`cmake .` builds use `cmake --build .` and `ctest` from the repository root. There is no
CMake install target; `install_SO.sh` expects the three shared libraries in `build/`.

## Deployment

Deployment requires pushing our `.so` files to the remote, distributed file system and then registering the functions with Impala via query. If we have already deployed and registered our functions and just want to update them, we can just push to the remote storage system and use Impala's `REFRESH FUNCTION` [syntax](https://impala.apache.org/docs/build/html/topics/impala_refresh_functions.html).

### Set Vars

Suppose we have an install location for the included libraries. We can set an environmental variable like so:

```bash
# Set HDFS (or other store) location for the libraries
export UDF_BIOUTILS_PATH="/udx/ncird_id/prod"

```

We used a folder called `/udx` for both UDF and UDAs, a prod vs. dev subfolder, and a group for our organization which is NCIRD/ID. In practice you should set this to where you expect the UDFs to be readable by Impala. If they are not readable, adjust permissions and/or [Ranger](https://docs.cloudera.com/cdp-reference-architectures/latest/cdp-ra-security/topics/cdp-ra-security-apache-ranger.html) policies until they are.

### Push to the Remote

The library artifacts are in the build directory and we can use the Hadoop CLI to push them to our remote file system (in combination with our environment variable for the install location). See:

```bash
# Set path to the SO location for your data lake
export UDF_BIOUTILS_PATH="/udx/prod"
./install_SO.sh
```

### Generate and run SQL

Next we can generate SQL to register the functions using our above variables:

```bash
# Set path to the SO location for your data lake
export UDF_BIOUTILS_PATH="/udx/prod"

# Creates tmp files to execute in Impala
for i in sql/*;do
    cat $i|envsubst > $(basename $i .sql).tmp.sql
done
```

The query files can be executed via CLI or in something like HUE. Finally, it is recommended to use:

```sql
-- Supposing `udx` is the DB with your functions
refresh functions `udx`;
```
