/*
原始数据:
id,name,login_date
1,bebee,2019-12-01
2,bebee,2019-12-02
3,jack,2019-12-03
4,bebee,2019-12-04
5,bebee,2019-12-05
6,jack,2019-12-06
7,jack,2019-12-07
8,bebee,019-12-08

目标数据:
name    login_date
bebee   2019-12-01,2019-12-02,2019-12-04,2019-12-05,2019-12-08
jack    2019-12-03,2019-12-06,2019-12-07
*/

-- 1. 建表
set hive.exec.mode.local.auto=true;   --开启Hive的本地模式

drop table if exists user_login;
create temporary table user_login(
  id int,
  name string,
  login_date string
) row format delimited fields terminated by ','
stored as textfile
tblproperties(
  "skip.header.line.count"="1"  --跳过文件首的1行
  --"skip.footer.line.count"="n"  --跳过文件尾的n行
);

load data local inpath '/tmp/sgr/user_login.csv' into table user_login;

-- 2. 处理
select name,concat_ws(',',collect_set(login_date)) as login_date
from user_login
group by name;






