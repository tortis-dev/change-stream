using System.Data;
using System.Runtime.CompilerServices;
using Microsoft.Data.SqlClient;

namespace MilestoneTG.ChangeStream.SqlServer;

public class SqlServerSource : ISource, IDisposable, IAsyncDisposable
{
    readonly SqlConnection _sqlConnection;
    readonly GetChangesCommand _getChanges;
    readonly Journal _journal;
    readonly string _captureInstance;
    readonly byte[] _observedLsn = new byte[10];
    readonly byte[] _observedSeq = new byte[10];
    readonly byte[] _padding = new byte[6];

    public SqlServerSource(Dictionary<string, object> settings, IConnectionStringFactory connectionStringFactory)
    {
        _captureInstance = $"{settings[SqlServerChangeSourceSettings.SchemaName]}_{settings[SqlServerChangeSourceSettings.TableName]}";
        var connectionString = connectionStringFactory.GetConnectionString((string)settings[SqlServerChangeSourceSettings.ConnectionStringName]);
        _sqlConnection = new SqlConnection(connectionString);
        _getChanges = new GetChangesCommand(_sqlConnection, _captureInstance);
        _journal = new Journal(connectionString);
        _journal.EnsureCreated();
    }
    
    public async IAsyncEnumerable<ChangeEvent> GetChanges([EnumeratorCancellation] CancellationToken cancellationToken)
    {
        if (_sqlConnection.State == ConnectionState.Closed)
            await _sqlConnection.OpenAsync(cancellationToken);
        
        await using var rdr = await _getChanges.ExecuteAsync(cancellationToken);
        while (await rdr.ReadAsync(cancellationToken))
        {
            rdr.GetBytes(0, 0, _observedLsn, 0, 10);
            rdr.GetBytes(1, 0, _observedSeq, 0, 10);

            var changeEvent = new ChangeEvent
            {
                ChangeId = new Guid(_padding.Concat(_observedLsn).ToArray()),
                OperationSequence = new Guid(_padding.Concat(_observedSeq).ToArray()),
                Operation = (Operation)rdr.GetInt32(2), 
                Timestamp = DateTime.UtcNow
            };
            
            for (var f = 4; f < rdr.FieldCount; f++)
                changeEvent.Values.Add(rdr.GetName(f), rdr.GetValue(f));
            
            yield return changeEvent;

            await _journal.UpdateAsync(_captureInstance, _observedLsn, cancellationToken);
        }

        await _sqlConnection.CloseAsync();
    }

    public void Dispose()
    {
        _sqlConnection.Dispose();
        _getChanges.Dispose();
    }

    public async ValueTask DisposeAsync()
    {
        await _sqlConnection.DisposeAsync();
        await _getChanges.DisposeAsync();
    }
}
