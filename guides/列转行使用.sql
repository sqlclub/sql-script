/*
原始数据:
name,login_date
bebee,2019-12-01|2019-12-02|2019-12-04|2019-12-05|2019-12-08
jack,2019-12-03|2019-12-06|2019-12-07

目标数据:
name    login_date
bebee   2019-12-01
bebee   2019-12-02
jack    2019-12-03
bebee   2019-12-04
bebee   2019-12-05
jack    2019-12-06
jack    2019-12-07
bebee   2019-12-08

*/

-- 1. 建表
set hive.exec.mode.local.auto=true;   --开启Hive的本地模式

drop table if exists user_login;
create temporary table user_login(
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
select a.name,b.login_date
from user_login a
lateral view explode(split(login_date,'\\|')) b AS login_date
order by b.login_date;






