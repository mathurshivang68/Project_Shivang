6) Find the percentage and the count of each case status on total applications for each year. Create a line graph depicting the pattern of All the cases over the period of time. 



REGISTER '/home/hduser/ExternalJars/piggybank-0.13.0.jar'; --Register external jar 'Piggy Bank.jar'

DEFINE CSVExcelStorage org.apache.pig.piggybank.storage.CSVExcelStorage;  -- within the jar define a function CSVExcelStorage()  

data = LOAD '/home/hduser/h1b.csv' USING CSVExcelStorage() as 
(s_no:int,
case_status:chararray,
employer_name:chararray,
soc_name:chararray,
job_title:chararray,
full_time_position:chararray,
prevailing_wage:int,
year:chararray,
worksite:chararray,
longitute:double,
latitute:double);			--Load data

noheader= filter data by $0>=1;		--Remove header 

certified_2011= filter noheader by $1 matches 'CERTIFIED' and $7 matches '2011';
withdrawn_2011= filter noheader by $1 matches 'WITHDRAWN' and $7 matches '2011';
denied_2011= filter noheader by $1 matches 'DENIED' and $7 matches '2011';
certifiedw_2011= filter noheader by $1 matches '.*CERTIFIED-WITHDRAWN*.' and $7 matches '2011';
a= group certified_2011 by $1;   --group by case status
b= group withdrawn_2011 by $1;
c= group denied_2011 by $1;
d= group certifiedw_2011 by $1;
count_c2011= foreach a generate '2011',group,COUNT($1);
count_w2011= foreach b generate '2011',group,COUNT($1);
count_d2011= foreach c generate '2011',group,COUNT($1);
count_cw2011= foreach d generate '2011',group,COUNT($1);
joinedd= join count_c2011 by $0, count_w2011 by $0, count_d2011 by $0, count_cw2011 by $0;
final_2011= foreach joinedd generate $0, $1, $2, $4, $5, $7, $8, $10, $11;
x= group final_2011 by $0;

--dump x;

total= foreach final_2011 generate $2+$4+$6+$8;
y= group total by $0;
--dump total;

joined1= join final_2011 by $0, total by $0;


cperc_2011= foreach joined1 generate $0, $1, ROUND_TO(($2/$2+$4+$6+$8)*100,2);






certified_2012= filter noheader by $1 matches 'CERTIFIED' and $7 matches '2012';
withdrawn_2012= filter noheader by $1 matches 'WITHDRAWN' and $7 matches '2012';
denied_2012= filter noheader by $1 matches 'DENIED' and $7 matches '2012';
certifiedw_2012= filter noheader by $1 matches 'CERTIFIED-WITHDRAWN' and $7 matches '2012';
a= group certified_2012 by $1;
b= group withdrawn_2012 by $1;
c= group denied_2012 by $1;
d= group certifiedw_2012 by $1;
count_c2012= foreach a generate '2012',group,COUNT($1);
count_w2012= foreach b generate '2012',group,COUNT($1);
count_d2012= foreach c generate '2012',group,COUNT($1);
count_cw2012= foreach d generate '2012',group,COUNT($1);
joinedd= join count_c2012 by $0, count_w2012 by $0, count_d2012 by $0, count_cw2012 by $0;
final_2012= foreach joinedd generate $0, $1, $2, $4, $5, $7, $8, $10, $11;
--dump final_2012;


certified_2013= filter noheader by $1 matches 'CERTIFIED' and $7 matches '2013';
withdrawn_2013= filter noheader by $1 matches 'WITHDRAWN' and $7 matches '2013';
denied_2013= filter noheader by $1 matches 'DENIED' and $7 matches '2013';
certifiedw_2013= filter noheader by $1 matches 'CERTIFIED-WITHDRAWN' and $7 matches '2013';
a= group certified_2013 by $1;
b= group withdrawn_2013 by $1;
c= group denied_2013 by $1;
d= group certifiedw_2013 by $1;
count_c2013= foreach a generate '2013',group,COUNT($1);
count_w2013= foreach b generate '2013',group,COUNT($1);
count_d2013= foreach c generate '2013',group,COUNT($1);
count_cw2013= foreach d generate '2013',group,COUNT($1);
joinedd= join count_c2013 by $0, count_w2013 by $0, count_d2013 by $0, count_cw2013 by $0;
final_2013= foreach joinedd generate $0, $1, $2, $4, $5, $7, $8, $10, $11;
--dump final_2013;


certified_2014= filter noheader by $1 matches 'CERTIFIED' and $7 matches '2014';
withdrawn_2014= filter noheader by $1 matches 'WITHDRAWN' and $7 matches '2014';
denied_2014= filter noheader by $1 matches 'DENIED' and $7 matches '2014';
certifiedw_2014= filter noheader by $1 matches 'CERTIFIED-WITHDRAWN' and $7 matches '2014';
a= group certified_2014 by $1;
b= group withdrawn_2014 by $1;
c= group denied_2014 by $1;
d= group certifiedw_2014 by $1;
count_c2014= foreach a generate '2014',group,COUNT($1);
count_w2014= foreach b generate '2014',group,COUNT($1);
count_d2014= foreach c generate '2014',group,COUNT($1);
count_cw2014= foreach d generate '2014',group,COUNT($1);
joinedd= join count_c2014 by $0, count_w2014 by $0, count_d2014 by $0, count_cw2014 by $0;
final_2014= foreach joinedd generate $0, $1, $2, $4, $5, $7, $8, $10, $11;
--dump final_2014;


certified_2015= filter noheader by $1 matches 'CERTIFIED' and $7 matches '2015';
withdrawn_2015= filter noheader by $1 matches 'WITHDRAWN' and $7 matches '2015';
denied_2015= filter noheader by $1 matches 'DENIED' and $7 matches '2015';
certifiedw_2015= filter noheader by $1 matches 'CERTIFIED-WITHDRAWN' and $7 matches '2015';
a= group certified_2015 by $1;
b= group withdrawn_2015 by $1;
c= group denied_2015 by $1;
d= group certifiedw_2015 by $1;
count_c2015= foreach a generate '2015',group,COUNT($1);
count_w2015= foreach b generate '2015',group,COUNT($1);
count_d2015= foreach c generate '2015',group,COUNT($1);
count_cw2015= foreach d generate '2015',group,COUNT($1);
joinedd= join count_c2014 by $0, count_w2015 by $0, count_d2015 by $0, count_cw2015 by $0;
final_2015= foreach joinedd generate $0, $1, $2, $4, $5, $7, $8, $10, $11;
--dump final_2015;


certified_2016= filter noheader by $1 matches 'CERTIFIED' and $7 matches '2016';
withdrawn_2016= filter noheader by $1 matches 'WITHDRAWN' and $7 matches '2016';
denied_2016= filter noheader by $1 matches 'DENIED' and $7 matches '2016';
certifiedw_2016= filter noheader by $1 matches 'CERTIFIED-WITHDRAWN' and $7 matches '2016';
a= group certified_2016 by $1;
b= group withdrawn_2016 by $1;
c= group denied_2016 by $1;
d= group certifiedw_2016 by $1;
count_c2016= foreach a generate '2016',group,COUNT($1);
count_w2016= foreach b generate '2016',group,COUNT($1);
count_d2016= foreach c generate '2016',group,COUNT($1);
count_cw2016= foreach d generate '2016',group,COUNT($1);
joinedd= join count_c2016 by $0, count_w2016 by $0, count_d2016 by $0, count_cw2016 by $0;
final_2016= foreach joinedd generate $0, $1, $2, $4, $5, $7, $8, $10, $11;
--dump final_2016


