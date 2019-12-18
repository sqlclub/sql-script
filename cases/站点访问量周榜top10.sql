/*
需求:
要求统计一周内每日访问量pv最高的站点
数据如下:
day,website,pv
2019-10-01,www.blogtech.top,13
2019-10-01,www.sqlfun.cn,10
2019-10-01,www.sqlpower.top,9
2019-10-01,www.sqlpower.tech,15
2019-10-02,www.blogtech.top,13
2019-10-02,www.sqlfun.cn,9
2019-10-02,www.sqlpower.top,9
2019-10-03,www.sqlpower.tech,15
2019-10-04,www.blogtech.top,13
2019-10-04,www.sqlfun.cn,9
2019-10-05,www.sqlpower.top,9
2019-10-06,www.sqlpower.tech,15
2019-10-07,www.blogtech.top,13
2019-10-07,www.sqlfun.cn,10
*/

-- 1. 建表
set hive.exec.mode.local.auto=true;   --开启Hive的本地模式

drop table if exists user_website_pv;
create temporary table user_website_pv(
  day string,
  website string,
  pv int
) row format delimited fields terminated by ','
stored as textfile
tblproperties(
  "skip.header.line.count"="1"  --跳过文件首的1行
  --"skip.footer.line.count"="n"  --跳过文件尾的n行
);

load data local inpath '/tmp/sgr/user_website_pv.csv' into table user_website_pv;

-- 2. 处理

select a.day,a.website,a.sum_pv from (
  select  day,website,sum(pv) as sum_pv,
  row_number() over(partition by day order by sum(pv) desc) rank
   from user_website_pv where day
  BETWEEN date_sub('2019-10-07', 7) and '2019-10-07'
  group by day,website
) a where a.rank=1;


/*
结果:
2019-10-01	www.sqlpower.tech	15
2019-10-02	www.blogtech.top	13
2019-10-03	www.sqlpower.tech	15
2019-10-04	www.blogtech.top	13
2019-10-05	www.sqlpower.top	9
2019-10-06	www.sqlpower.tech	15
2019-10-07	www.blogtech.top	13
*/




