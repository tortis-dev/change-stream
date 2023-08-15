using System.Data;
using Microsoft.Data.SqlClient;

namespace MilestoneTG.ChangeStream.Server.SqlServer;

sealed class Journal
{
    readonly SqlConnection _connection;
    public Journal(SqlConnection connection)
    {
        _connection = connection;
        _updateCommand = new SqlCommand(UPDATE_SQL, _connection);
        _updateCommand.Parameters.Add(new SqlParameter("@capture_instance_name", SqlDbType.NVarChar, 128));
        _updateCommand.Parameters.Add(new SqlParameter("@last_published_lsn", SqlDbType.Binary, 10));
    }

    readonly SqlCommand _updateCommand;
    const string UPDATE_SQL = 
        @"merge into milestone_cdc.journal as tgt
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

        await _updateCommand.ExecuteNonQueryAsync(cancellationToken);
    }
    
    public async Task EnsureCreatedAsync(CancellationToken cancellationToken)
    {
        const string schemaExistsSql = 
            @"if not exists (select 1 from sys.schemas s where s.name = 'milestone_cdc')
                 exec('create schema milestone_cdc');";
        
        await using var ensureSchema = new SqlCommand(schemaExistsSql, _connection);
        await ensureSchema.ExecuteNonQueryAsync(cancellationToken);

        const string tableExistsSql =
            @"if not exists (select 1 from sys.schemas s join sys.objects o on s.schema_id = o.schema_id where s.name = 'milestone_cdc' and o.name = 'journal')
                create table milestone_cdc.journal (
                    capture_instance_name nvarchar(128) not null constraint pk_journal primary key,
                    last_published_lsn binary(10)
                );";

        await using var ensureTable = new SqlCommand(tableExistsSql, _connection);
        await ensureTable.ExecuteNonQueryAsync(cancellationToken);
    }
}