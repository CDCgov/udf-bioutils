# Installation

First we install necessary packages via `yum` and `pip`:

```{bash}
sudo yum install epel-release
sudo yum install gcc-c++ clang-devel boost-devel
sudo yum install impala-udf-devel
pip3.8  install cmake --user
```

We need to use GCC 9. This provides the C++20 standard but carefully uses static linking to avoid linker errors at runtime:

```{bash}
sudo yum install centos-release-scl
sudo yum-config-manager --enable rhel-server-rhscl-9-rpms
sudo yum install devtoolset-9
scl enable devtoolset-9 bash
```

For `impala-udf-devel`, a comparable major/minor version of CDH may be needed. In the past this repo was available publicly but Cloudera seems to no longer fully support it--despite its documentation--and yum packages are behind the paywall (not for every version, maybe just for patch builds). One can use the credentials in the Cloudera Manager yum repo to create a repo for `impala-udf-devel`. Client nodes may already have this installed.

If one is on a client node and cannot get the yum repo working (or if there is a parity issue), one can try to link explicitly with *root*. Soft link the source files:

```{bash}
/opt/cloudera/parcels/CDH/include/impala_udf/uda-test-harness.h
/opt/cloudera/parcels/CDH/include/impala_udf/uda-test-harness-impl.h
/opt/cloudera/parcels/CDH/include/impala_udf/udf-debug.h
/opt/cloudera/parcels/CDH/include/impala_udf/udf.h
/opt/cloudera/parcels/CDH/include/impala_udf/udf-test-harness.h
/opt/cloudera/parcels/CDH/lib64/libImpalaUdf-debug.a
/opt/cloudera/parcels/CDH/lib64/libImpalaUdf-retail.a
```

To the system path destination files:

```{bash}
/usr/include/impala_udf/uda-test-harness.h
/usr/include/impala_udf/uda-test-harness-impl.h
/usr/include/impala_udf/udf-debug.h
/usr/include/impala_udf/udf.h
/usr/include/impala_udf/udf-test-harness.h
/usr/lib64/libImpalaUdf-debug.a
/usr/lib64/libImpalaUdf-retail.a
```

If one does not have root access, then a user directory will have to be used and the compile options adjusted accordingly.

## Compilation

I have altered the `CMakeLists.txt` file to require C++20 for both the c++ compiler (emits .so) and for clang (emits IR or .ll for llvm). To compile from scratch just use:

```{bash}
cmake3 .
make
```

Files will need to be put in HDFS and then instantiated within Impala using the provided SQL. Apparently both the IR code and the shared object code can be used, but the former is more compatible. I do not know if there is any performance difference between the two and have only used the SO files.
