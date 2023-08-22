using System.Data;
using Microsoft.Data.SqlClient;
using Microsoft.SqlServer.Management.Common;

namespace MilestoneTG.ChangeStream.SqlServer;

sealed class Journal
{
    readonly SqlConnection _connection;
    public Journal(string connectionString)
    {
        _connection = new SqlConnection(connectionString);
        _updateCommand = new SqlCommand(UPDATE_SQL, _connection);
        _updateCommand.Parameters.Add(new SqlParameter("@capture_instance_name", SqlDbType.NVarChar, 128));
        _updateCommand.Parameters.Add(new SqlParameter("@last_published_lsn", SqlDbType.Binary, 10));
    }

    readonly SqlCommand _updateCommand;
    const string UPDATE_SQL = 
        @"merge into milestone_cdc.cdc_journal as tgt
            using (values(@capture_instance_name, @last_published_lsn)) as src (capture_instance_name, last_published_lsn)
            on tgt.capture_instance_name = src.capture_instance_name
          when matched then
            update set last_published_lsn = src.last_published_lsn
          when not matched then
            insert (capture_instance_name, last_published_lsn) values (src.capture_instance_name, src.last_published_lsn);";

    public async Task UpdateAsync(string captureInstanceName, byte[] lastPublishedLsn, CancellationToken cancellationToken)
    {
        _updateCommand.Parameters["@capture_instance_name"].Value = captureInstanceName;
        _updateCommand.Parameters["@last_published_lsn"].Value = lastPublishedLsn;

        await _connection.OpenAsync(cancellationToken);
        await _updateCommand.ExecuteNonQueryAsync(cancellationToken);
        await _connection.CloseAsync();
    }
    
    public void EnsureCreated()
    {
        _connection.Open();
        var sqlServer = new Microsoft.SqlServer.Management.Smo.Server(new ServerConnection(_connection));
        const string schemaExistsSql = 
            @"if not exists (select 1 from sys.schemas s where s.name = 'milestone_cdc')
                 exec('create schema milestone_cdc');";
        
        using var ensureSchema = new SqlCommand(schemaExistsSql, _connection);
        ensureSchema.ExecuteNonQuery();

        const string tableExistsSql =
            @"if not exists (select 1 from sys.schemas s join sys.objects o on s.schema_id = o.schema_id where s.name = 'milestone_cdc' and o.name = 'cdc_journal')
                create table milestone_cdc.cdc_journal (
                    capture_instance_name nvarchar(128) not null constraint pk_cdc_journal primary key clustered,
                    last_published_lsn binary(10)
                );";

        using var ensureTable = new SqlCommand(tableExistsSql, _connection);
        ensureTable.ExecuteNonQuery();

        const string functionExists =
            @"if exists (select 1 from sys.schemas s join sys.objects o on s.schema_id = o.schema_id where s.name = 'milestone_cdc' and o.name = 'fn_get_starting_lsn')
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
            end";
        
        sqlServer.ConnectionContext.ExecuteNonQuery(functionExists);
        _connection.Close();
    }
}