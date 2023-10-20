if not exists (select 1 from sys.schemas s where s.name = 'milestone_cdc')
    exec('create schema milestone_cdc');
go

if not exists (select 1 from sys.schemas s join sys.objects o on s.schema_id = o.schema_id where s.name = 'milestone_cdc' and o.name = 'cdc_journal')
    create table milestone_cdc.cdc_journal (
       capture_instance_name nvarchar(128) not null constraint pk_cdc_journal primary key clustered,
       last_published_lsn binary(10)
    )
go

if exists (select 1 from sys.schemas s join sys.objects o on s.schema_id = o.schema_id where s.name = 'milestone_cdc' and o.name = 'fn_get_starting_lsn')
    drop function milestone_cdc.fn_get_starting_lsn;
go

create function milestone_cdc.fn_get_starting_lsn(@capture_instance nvarchar(128))
    returns binary(10)
as
begin
    declare @lastlsn binary(10), @minlsn binary(10), @fromlsn binary(10)

    select @lastlsn = last_published_lsn from milestone_cdc.cdc_journal where capture_instance_name = @capture_instance;
    set @minlsn = sys.fn_cdc_get_min_lsn(@capture_instance);

    if (@lastlsn is null or @lastlsn < @minlsn)
        set @fromlsn = @minlsn;
    else
        set @fromlsn = sys.fn_cdc_increment_lsn (@lastlsn);

return @fromlsn;
end
