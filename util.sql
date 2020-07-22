
select sn,sum(file_length)/1024 from 
lk_device_dfiles a,lk_device_mfiles b where a.batch_no=b.batch_no and b.cmd_type='pushfile'
and date_format(create_time,'%Y-%m')='2020-05'
group by sn

select sn,sum(file_length)/1024 from 
lk_device_dfiles a,lk_device_mfiles b where a.batch_no=b.batch_no and b.cmd_type!='pushfile'
and date_format(create_time,'%Y-%m')='2020-05'
group by sn