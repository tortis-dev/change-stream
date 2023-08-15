using System.Data;
using Microsoft.Data.SqlClient;

namespace MilestoneTG.ChangeStream.Server.SqlServer;

sealed class GetChangesCommand : IDisposable, IAsyncDisposable
{
    readonly SqlCommand _getChanges;

    public GetChangesCommand(SqlConnection connection, string captureInstance)
    {
        _getChanges = 
            new SqlCommand(
                @$"declare @fromLsn binary(10), @toLsn binary(10);
                            set @fromLsn = milestone_cdc.fn_get_starting_lsn(@captureInstance);
                            set @toLsn = sys.fn_cdc_get_max_lsn();
                            if (@fromLsn <= @toLsn)
                                select * from cdc.fn_cdc_get_all_changes_{captureInstance}(@fromLsn, @toLsn, N'all');", 
                connection);
        
        _getChanges.Parameters.Add("@captureInstance", SqlDbType.NVarChar, 128).Value = captureInstance;
    }

    public async Task<SqlDataReader> ExecuteAsync(CancellationToken cancellationToken)
    {
        return await _getChanges.ExecuteReaderAsync(cancellationToken);
    }

    public void Dispose()
    {
        _getChanges.Dispose();
    }

    public ValueTask DisposeAsync()
    {
        return _getChanges.DisposeAsync();
    }
}