using System.Data;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;

namespace MilestoneTG.ChangeStream.SqlServer;

public class SqlServerChangeSource : ChangeSourceBase<SqlServerChangeSource>
{
    readonly SqlServerChangeSourceSettings _settings;
    public SqlServerChangeSource(SqlServerChangeSourceSettings settings, ILogger<SqlServerChangeSource> logger) 
        : base(logger)
    {
        logger.BeginScope("{schema}.{table}", settings.SchemaName, settings.TableName);
        _settings = settings;
    }
    
    protected override async Task Worker(CancellationToken cancellationToken)
    {
        if (_settings == null || string.IsNullOrWhiteSpace(_settings.SchemaName) ||
            string.IsNullOrWhiteSpace(_settings.TableName))
            throw new ApplicationException("Not configured.");

        string schemaName = _settings.SchemaName;
        string tableName = _settings.TableName;
        string captureInstance = $"{schemaName}_{tableName}";
        
        SqlConnection sqlConnection = new SqlConnection();
        var getChanges = new GetChangesCommand(sqlConnection, captureInstance);
        try
        {
            sqlConnection.ConnectionString = _settings.ConnectionString;
            sqlConnection.StateChange += OnSqlConnectionStateChange;
            

            var journal = new Journal(sqlConnection);
            await sqlConnection.OpenAsync(cancellationToken);
            await journal.EnsureCreatedAsync(cancellationToken);
            await sqlConnection.CloseAsync();
            
            Logger.LogInformation("SQL Server CDC monitoring for table {table} in schema {schema} started.", tableName,
                schemaName);

            var observedLsn = new byte[10];
            bool changesFound;
            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    await sqlConnection.OpenAsync(cancellationToken);
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
                catch (SqlException ex)
                {
                    Logger.LogError(ex, "SQL Server error: {error}", ex.Number);
                }
                finally
                {
                    await sqlConnection.CloseAsync();
                }

                await Task.Delay(_settings.IntervalInMilliseconds, cancellationToken);
            }
        }
        catch (TaskCanceledException)
        {
            Logger.LogInformation("Stopping SQL Server CDC monitoring for table {table} in schema {schema}.", tableName,
                schemaName);
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

    void OnSqlConnectionStateChange(object sender, StateChangeEventArgs e)
    {
        if (e.CurrentState == ConnectionState.Broken)
        {
            Logger.LogDebug($"Connection state changed from {e.OriginalState} to {e.CurrentState}");
            ((SqlConnection)sender).Open();
        }
    }
}
