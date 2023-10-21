using System.Data;
using System.Numerics;
using System.Runtime.CompilerServices;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;

namespace Tortis.ChangeStream.SqlServer;

[PublicAPI]
[UsedImplicitly]
public class SqlServerSource : ISource
{
    static readonly string CONNECTION_STRING_NAME = "ConnectionStringKey";
    static readonly string SCHEMA_NAME = "SchemaName";
    static readonly string TABLE_NAME = "TableName";

    SqlConnection? _sqlConnection;
    GetChangesCommand? _getChanges;
    Journal? _journal;
    string? _captureInstance;
    byte[] _observedLsn = new byte[10];
    byte[] _observedSeq = new byte[10];
    byte[] _maskBuffer = new byte[8];

    ///<inheritDoc />
    public void Configure(Dictionary<string, object> settings, IConnectionStringFactory connectionStringFactory, ILoggerFactory loggerFactory)
    {
        _captureInstance = $"{settings[SCHEMA_NAME]}_{settings[TABLE_NAME]}";
        var connectionString = connectionStringFactory.GetConnectionString((string)settings[CONNECTION_STRING_NAME]);
        _sqlConnection = new SqlConnection(connectionString);
        _getChanges = new GetChangesCommand(_sqlConnection, _captureInstance, loggerFactory);
        _journal = new Journal(connectionString);
        _journal.EnsureCreated();
    }

    static readonly string EVENT_ID_FORMAT = new('0', 49);
    static readonly byte[] SIGN_BYTE = { 0x00 };

    const int LSN_POSITION = 0;
    const int SEQ_POSITION = 1;
    const int OPERATION_POSITION = 2;
    const int CHANGE_MASK_POSITION = 3;
    const int FIRST_FIELD_POSITION = 4;

    ///<inheritDoc />
    public async IAsyncEnumerable<ChangeEvent> GetChanges([EnumeratorCancellation] CancellationToken cancellationToken)
    {
        if (_sqlConnection is null || _getChanges is null || _journal is null || string.IsNullOrWhiteSpace(_captureInstance))
            throw new InvalidOperationException("The source is not configured. Make sure Configure() is called before calling GetChanges().");

        if (_sqlConnection.State == ConnectionState.Closed)
            await _sqlConnection.OpenAsync(cancellationToken);

        await using var rdr = await _getChanges.ExecuteAsync(cancellationToken);
        if (rdr is null)
            yield break;

        while (await rdr.ReadAsync(cancellationToken))
        {
            rdr.GetBytes(LSN_POSITION, 0, _observedLsn, 0, 10);
            rdr.GetBytes(SEQ_POSITION, 0, _observedSeq, 0, 10);

            var changeEvent = new ChangeEvent
            {
                //little-endian. Least significant byte needs to be at position 0 of the array.
                // Example: SQL LSN of 0000009B00004DD00008
                // Least significant byte ↓↓
                //      0000009B00004DD00008
                // Unfortunately, when read from the reader into a byte array using rdr.GetBytes(),
                // 0000009B00004DD00008
                // ↑↑ This becomes the least significant byte (element 0).
                // So, to get the correct base10 value, we must reverse the array so it's in the correct endianness.
                // Then we get the correct base10 value of:
                // 43628622695628808
                // You can test it here: https://www.rapidtables.com/convert/number/hex-to-decimal.html
                // BigInteger can handle big-endian, so we don't have to reverse the array ourselves and save on the allocation.
                EventId = new BigInteger(_observedLsn.Concat(_observedSeq).ToArray(), true, true).ToString(EVENT_ID_FORMAT),
                Operation = (Operation)rdr.GetInt32(OPERATION_POSITION),
                Timestamp = DateTime.UtcNow
            };

            rdr.GetBytes(3, 0, _maskBuffer, 0, 8);

            var mask = new BigInteger(_maskBuffer);
            for (var f = FIRST_FIELD_POSITION; f < rdr.FieldCount; f++)
            {
                var bit = BigInteger.Pow(2, f - 4);
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

    ///<inheritDoc />
    public void Dispose()
    {
        _sqlConnection?.Dispose();
        _getChanges?.Dispose();
        _journal?.Dispose();
    }
}
