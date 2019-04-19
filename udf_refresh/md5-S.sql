drop function if exists udx.md5(string ...);
create function udx.md5(string ...) returns string location "/user/vfn4/udx/libudfbioutils.so" SYMBOL="md5";

