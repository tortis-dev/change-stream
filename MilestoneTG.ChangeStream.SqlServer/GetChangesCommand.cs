using System.Data;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;

namespace MilestoneTG.ChangeStream.SqlServer;

sealed class GetChangesCommand : IDisposable, IAsyncDisposable
{
    readonly SqlCommand _getChanges;
    readonly ILogger<GetChangesCommand> _logger;
    readonly string _captureInstance;

    public GetChangesCommand(SqlConnection connection, string captureInstance, ILoggerFactory loggerFactory)
    {
        _captureInstance = captureInstance;
        _logger = loggerFactory.CreateLogger<GetChangesCommand>();
        
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

    public async Task<SqlDataReader?> ExecuteAsync(CancellationToken cancellationToken)
    {
        try
        {
            return await _getChanges.ExecuteReaderAsync(cancellationToken);
        }
        catch (SqlException e) when (e.ErrorCode == 313)
        {
            _logger.LogDebug(e, "SQL Error 313 while trying to fetch changes for capture instance {captureInstance} This is expected.", _captureInstance);
            return null;
        }
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