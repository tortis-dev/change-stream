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
