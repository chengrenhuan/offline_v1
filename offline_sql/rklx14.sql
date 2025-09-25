// outhor Eric
//date 9/25
create database if not exists rklx;
use rklx;
drop table if exists students;
create table if not exists students(
    student_id string,
    student_name string,
    class_id string
)
row format delimited
fields terminated by ","
stored as textfile ;
load data  inpath 'hdfs://cdh01:8020/lx/rk14/students.txt' into table students;
select * from students;
drop table if exists courses;
create table if not exists courses(
    course_id string,
    course_name string
)
row format delimited
fields terminated by ","
stored as textfile ;
LOAD DATA  INPATH 'hdfs://cdh01:8020/lx/rk14/courses.txt' INTO TABLE courses;

select * from courses;
drop table if exists scores;
create table if not exists scores(
    student_id string,
    course_id string,
    score string
)
row format delimited
fields terminated by ","
stored as textfile ;
load data inpath 'hdfs://cdh01:8020/lx/rk14/scores.txt'into table scores;
select * from scores;
select s.student_id,student_name,sum(score) from students s
         join rklx.scores s2 on s.student_id = s2.student_id
group by s.student_id,student_name;

select
    c.course_id,c.course_name,avg(s.score)
from courses c
join rklx.scores s on c.course_id = s.course_id
group by c.course_id,c.course_name;

select
    *
from (
select
    t.student_id,t.student_name,rank() over (partition by c.course_id order by sum(score) desc )r1
from rklx.students t
join rklx.scores s on t.student_id = s.student_id
join rklx.courses c on s.course_id = c.course_id
group by t.student_id,t.student_name,c.course_id
)t
where t.r1=1;


select
    *
from (
         select
             t.student_id,t.student_name,sum(score),rank() over (partition by c.course_id order by sum(score)  )r1
         from rklx.students t
                  join rklx.scores s on t.student_id = s.student_id
                  join rklx.courses c on s.course_id = c.course_id
         group by t.student_id,t.student_name,c.course_id
     )t
where t.r1=1;


select
    class_id,avg(s2.score)
from students s
join rklx.scores s2 on s.student_id = s2.student_id
group by class_id;


select
    course_name,round((sum(if(s.score>=60,1,0))/count(distinct student_id)),2)

from courses c
join scores s on c.course_id = s.course_id
where course_name='English'
group by course_name;



select
    t.student_id,t.student_name,
    avg(s.score)
from students t
join rklx.scores s on t.student_id = s.student_id
join rklx.courses c on s.course_id = c.course_id
group by t.student_id,t.student_name;

select
    *
from (select t.student_id,
             t.student_name,
             c.course_id,
             c.course_name,
             rank() over (partition by c.course_id,c.course_name order by sum(s.score))r1
      from students t
               join rklx.scores s on t.student_id = s.student_id
               join rklx.courses c on s.course_id = c.course_id
      group by t.student_id, t.student_name, c.course_id, c.course_name)t1
where t1.r1<=3;


select c.course_id,c.course_name,count(distinct s.student_id)
from scores s
join courses c on s.course_id = c.course_id
where score<60
group by c.course_id,c.course_name;

select student_id,max(s.score)
from students t
join rklx.scores s on t.student_id = s.student_id
join rklx.courses c on s.course_id = c.course_id
group by s.student_id;


