#11) Export result for question no 10 to MySql database.
Create a Database in mysql and create a table in it


create database if not exists final;
use final;
create table question11(job_title varchar(100),success_rate float,petitions int);';

sqoop export --connect jdbc:mysql://localhost/final --username root --password '1234' --table question11 --update-mode allowinsert  --export-dir /Pig/Question10/ --input-fields-terminated-by ',' ;

