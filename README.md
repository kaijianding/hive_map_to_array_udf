hive map_to_array udf

usages:

```sql
add jar maptoarray-udf-1.0.jar;
create temporary function map_to_array as 'com.didichuxing.hive.udf.MapToArrayUDF';
show functions;
select map_to_array(map("user","u1","age","22"));