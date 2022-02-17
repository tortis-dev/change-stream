create table person (
	id bigint not null identity(1,1) primary key,
	first_name nvarchar(20),
	last_name  nvarchar(50),
	email      nvarchar(128)
);
go

insert into person (first_name,last_name,email)
values (N'Elmer',N'Fudd',N'elmer.fudd@acme.com');
go

DECLARE @from_lsn binary(10), @to_lsn binary(10);  
SET @from_lsn = sys.fn_cdc_get_min_lsn('dbo_person');  
SET @to_lsn   = coalesce(sys.fn_cdc_get_max_lsn(), @from_lsn);
select @from_lsn, @to_lsn;
SELECT * FROM cdc.fn_cdc_get_all_changes_dbo_person(@from_lsn, @to_lsn, N'all');  
GO  

select sys.fn_cdc_get_min_lsn('dbo_person') as from_lsn,
	   coalesce(sys.fn_cdc_get_max_lsn(), sys.fn_cdc_get_min_lsn('dbo_person')) as to_lsn;

SET @to_lsn   = coalesce(sys.fn_cdc_get_max_lsn(), @from_lsn);


insert into person (first_name,last_name,email)
values (N'Jane',N'Smithe',N'jane.smithe@acme.com');
go

update person
set last_name = 'Public'
where id = 4;

create table next_lsn (
	table_name	nvarchar(128) not null primary key,
	next_lsn	binary(10) not null
);
go

insert into next_lsn (table_name, next_lsn)
values (N'dbo_person', sys.fn_cdc_get_min_lsn('dbo_person'));

update next_lsn 
set next_lsn = sys.fn_cdc_get_min_lsn('dbo_person')
where table_name = N'dbo_person';


select * from next_lsn;

truncate table next_lsn;
