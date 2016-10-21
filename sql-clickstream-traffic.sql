# Query clickstream data on redshift on join on own table created earlier in the process

query =\
"""
select  
	a.hitdatetime,
	a.adid,
	a.applicationname,
	a.hitduration,
	b.loginid,
	b.jobid,
	b.min_date,
	b.max_date
from
	((select 
	    hitdatetime, 
		loginid,
		adid,
		pageeventtypeid,
		applicationname,
		hitduration
	from 
	    %s
	where
		pageeventtypeid=0
	and adid!=0
	)
	union all     
	(select 
	    hitdatetime, 
		loginid,
		adid,
		pageeventtypeid,
		applicationname,
		hitduration
	from 
	    %s
	where
	    pageeventtypeid=0
	and adid!=0
	)) as a 
	
	inner join 
	
	(select 
		jobid,
		loginid,
		min_date,
		max_date
	from 
		analytic.ma_smajobber_crawlspace 
	where
		date_part('month', min_date)=%s
	and date_part('month', max_date)<=%s
	and date_part('year', min_date)=%s
	) as b
	
	on
		a.loginid = b.loginid 
	and a.hitdatetime between b.min_date and b.max_date
""" 
