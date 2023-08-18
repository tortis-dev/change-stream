using System.Data;
using Microsoft.Data.SqlClient;

namespace MilestoneTG.ChangeStream.Server.SqlServer;

public class SqlServerChangeSource : ChangeSourceBase<SqlServerChangeSource>
{
    public SqlServerChangeSource(IConnectionStringFactory connectionStringFactory, ILogger<SqlServerChangeSource> logger) 
        : base(connectionStringFactory, logger)
    {
    }
    
    protected override async Task Worker(CancellationToken cancellationToken)
    {
        if (Settings == null || string.IsNullOrWhiteSpace(Settings.SchemaName) ||
            string.IsNullOrWhiteSpace(Settings.TableName))
            throw new ApplicationException("Not configured.");

        string schemaName = Settings.SchemaName;
        string tableName = Settings.TableName;
        string captureInstance = $"{schemaName}_{tableName}";
        
        SqlConnection sqlConnection = new SqlConnection();
        var getChanges = new GetChangesCommand(sqlConnection, captureInstance);
        try
        {
            sqlConnection.ConnectionString = ConnectionStringFactory.GetConnectionString(Settings.SourceName);
            sqlConnection.StateChange += CnOnStateChange;
            await sqlConnection.OpenAsync(cancellationToken);

            var journal = new Journal(sqlConnection);
            await journal.EnsureCreatedAsync(cancellationToken);

            Logger.LogInformation("SQL Server CDC monitoring for table {table} in schema {schema} started.", tableName,
                schemaName);

            var observedLsn = new byte[10];
            bool changesFound;
            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    await using (var rdr = await getChanges.ExecuteAsync(cancellationToken))
                    {
                        changesFound = false;
                        while (await rdr.ReadAsync(cancellationToken))
                        {
                            changesFound = true;
                            rdr.GetBytes(0, 0, observedLsn, 0, 10);
                            var op = (Operation)rdr.GetInt32(2);

                            var changeEvent = new ChangeEvent { Operation = op, Timestamp = DateTime.UtcNow };
                            for (var f = 4; f < rdr.FieldCount; f++)
                                changeEvent.Values.Add(rdr.GetName(f), rdr.GetValue(f));

                            Publish(changeEvent);
                            Logger.LogDebug("Event appended to stream for operation {operation} on {table}",
                                changeEvent.Operation.ToString(), tableName);
                        }
                    }

                    if (changesFound)
                        await journal.UpdateAsync(captureInstance, observedLsn, cancellationToken);
                }
                catch (SqlException ex) when (ex.Number == 313)
                {
                    // expected. LSNs were outside the available range.
                    Logger.LogDebug(
                        "Log sequence numbers were outside the available range or invalid. This is usually because FromLsn was incremented to get the next change, and there are no changes, therefor FromLsn > ToLsn (max lsn) resulting in invalid input. SQL313.");
                }

                await Task.Delay(Settings.IntervalInMilliseconds, cancellationToken);
            }
        }
        catch (Exception ex)
        {
            Logger.LogCritical(ex, "A fatal error occurred while observing SQl Server CDC changes. See exception for details.");
            Error(ex);
        }
        finally
        {
            await sqlConnection.CloseAsync();
            await sqlConnection.DisposeAsync();
            await getChanges.DisposeAsync();

            Complete();

            Logger.LogInformation("SQL Server CDC monitoring for table {table} in schema {schema} stopped.", tableName,
                schemaName);
        }
    }

    void CnOnStateChange(object sender, StateChangeEventArgs e)
    {
        Logger.LogDebug($"Connection state changed from {e.OriginalState} to {e.CurrentState}");
        if (e.CurrentState == ConnectionState.Broken)
            ((SqlConnection)sender).Open();
    }
}
