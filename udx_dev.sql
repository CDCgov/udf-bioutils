
drop function if exists udx.dev_pcd(string,string);
create function if not exists udx.dev_pcd(string,string) returns double location "/udx/ncird_id/dev/libudfbioutils.so" SYMBOL="Physiochemical_Distance";
