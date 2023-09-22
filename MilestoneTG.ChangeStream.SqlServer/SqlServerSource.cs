﻿using System.Data;
using System.Numerics;
using System.Runtime.CompilerServices;
using JetBrains.Annotations;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;

namespace MilestoneTG.ChangeStream.SqlServer;

[PublicAPI]
[UsedImplicitly]
public class SqlServerSource : ISource
{
    static readonly string ConnectionStringName = "ConnectionStringName";
    static readonly string SchemaName = "SchemaName";
    static readonly string TableName = "TableName";
    
    readonly SqlConnection _sqlConnection;
    readonly GetChangesCommand _getChanges;
    readonly Journal _journal;
    readonly string _captureInstance;
    readonly byte[] _observedLsn = new byte[10];
    readonly byte[] _observedSeq = new byte[10];
    readonly byte[] _maskBuffer = new byte[8];

    public SqlServerSource(Dictionary<string, object> settings, IConnectionStringFactory connectionStringFactory, ILoggerFactory loggerFactory)
    {
        _captureInstance = $"{settings[SchemaName]}_{settings[TableName]}";
        var connectionString = connectionStringFactory.GetConnectionString((string)settings[ConnectionStringName]);
        _sqlConnection = new SqlConnection(connectionString);
        _getChanges = new GetChangesCommand(_sqlConnection, _captureInstance, loggerFactory);
        _journal = new Journal(connectionString);
        _journal.EnsureCreated();
    }

    static readonly string EventIdFormat = new String('0', 20);
    
    public async IAsyncEnumerable<ChangeEvent> GetChanges([EnumeratorCancellation] CancellationToken cancellationToken)
    {
        if (_sqlConnection.State == ConnectionState.Closed)
            await _sqlConnection.OpenAsync(cancellationToken);
        
        await using var rdr = await _getChanges.ExecuteAsync(cancellationToken);
        if (rdr is null)
            yield break;
        
        while (await rdr.ReadAsync(cancellationToken))
        {
            rdr.GetBytes(0, 0, _observedLsn, 0, 10);
            rdr.GetBytes(1, 0, _observedSeq, 0, 10);

            var changeEvent = new ChangeEvent
            {
                EventId = new BigInteger(_observedLsn.Concat(_observedSeq).ToArray()).ToString(EventIdFormat),
                Operation = (Operation)rdr.GetInt32(2), 
                Timestamp = DateTime.UtcNow
            };
            
            rdr.GetBytes(3, 0, _maskBuffer, 0, 8);

            var mask = new BigInteger(_maskBuffer);
            for (var f = 4; f < rdr.FieldCount; f++)
            {
                var bit = BigInteger.Pow(2, f-4);
                var changed = (mask & bit) != 0;
                if (changed)
                    changeEvent.Changes.Add(rdr.GetName(f), rdr.GetValue(f));
                
                changeEvent.Entity.Add(rdr.GetName(f), rdr.GetValue(f));
            }
            
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
}
