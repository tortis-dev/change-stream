using System.Reactive.Subjects;
using Microsoft.Data.SqlClient;

namespace MilestoneTG.ChangeStream.Server.SqlServer;

public class SqlServerChangeSource : IChangeSource
{
    readonly ILogger<SqlServerChangeSource> _logger;
    readonly ISubject<ChangeEvent> _changeStream = new Subject<ChangeEvent>();

    Task _cdcTask;
    readonly CancellationToken _cancellationToken;
    
    public SqlServerChangeSource(CancellationToken cancellationToken, ILoggerFactory loggerFactory)
    {
        _logger = loggerFactory.CreateLogger<SqlServerChangeSource>();
        _cancellationToken = cancellationToken;
        _cdcTask = Task.Factory.StartNew(StartAsync, _cancellationToken);
    }
    
    public IDisposable Subscribe(Action<ChangeEvent> onNext)
    {
        return _changeStream.Subscribe(onNext);
    }

    async Task StartAsync()
    {
        const string schema_name = "dbo";
        const string table_name = "person";
        const string capture_instance = $"{schema_name}_{table_name}";

        _logger.LogInformation("SQL Server CDC monitoring for table {table} in schema {schema} started.", table_name, schema_name);

        var cn = new SqlConnection(@"server=.\sqlexpress;database=change_stream_example;Integrated Security=SSPI;Encrypt=false");
        
        var getInitialLsns = new SqlCommand("select next_lsn from next_lsn where table_name = @table_name", cn);
        getInitialLsns.Parameters.Add(new SqlParameter("@table_name", System.Data.SqlDbType.NVarChar, 128) { Value = capture_instance } );

        var updateLsn = new SqlCommand("update next_lsn set next_lsn = @lsn where table_name = @table_name", cn);
        updateLsn.Parameters.Add("@lsn", System.Data.SqlDbType.Binary, 10);
        updateLsn.Parameters.Add(new SqlParameter("@table_name", System.Data.SqlDbType.NVarChar, 128) { Value = capture_instance });

        var getLatestLsn = new SqlCommand("select sys.fn_cdc_get_max_lsn() as to_lsn", cn);
        var getNextLsn = new SqlCommand("select sys.fn_cdc_increment_lsn(@lastLsn)", cn);
        getNextLsn.Parameters.Add("@lastLsn", System.Data.SqlDbType.Binary, 10);
       
        var getChanges = new SqlCommand($"select * from cdc.fn_cdc_get_all_changes_{capture_instance}( @from_lsn , @to_lsn , N'all' )", cn);
        getChanges.Parameters.Add("@from_lsn", System.Data.SqlDbType.Binary, 10);
        getChanges.Parameters.Add("@to_lsn", System.Data.SqlDbType.Binary, 10);

        var fromLsn = new byte[10];
        var toLsn = new byte[10];

        await cn.OpenAsync();
 
        using (var rdr = await getInitialLsns.ExecuteReaderAsync())
        {
            if (await rdr.ReadAsync())
                rdr.GetBytes(0, 0, fromLsn, 0, 10);
        }

        while (!_cancellationToken.IsCancellationRequested)
        {
            using (var rdr = getLatestLsn.ExecuteReader())
            {
                if (rdr.Read())
                    rdr.GetBytes(0, 0, toLsn, 0, 10);
            }

            //Console.WriteLine("From: {0} To: {1}", Convert.ToHexString(fromLsn), Convert.ToHexString(toLsn));
            getChanges.Parameters["@from_lsn"].Value = fromLsn;
            getChanges.Parameters["@to_lsn"].Value = toLsn;

            try
            {
                var changesFound = false;
                using (var rdr = getChanges.ExecuteReader())
                {
                    Operation op;
                    while (rdr.Read())
                    {
                        changesFound = true;
                        rdr.GetBytes(0, 0, fromLsn, 0, 10);
                        op = (Operation)rdr.GetInt32(2);

                        var changeEvent = new ChangeEvent { Operation = op };
                        for (var f = 4; f < rdr.FieldCount; f++)
                            changeEvent.Values.Add(rdr.GetName(f), rdr.GetValue(f));

                     
                        _changeStream.OnNext(changeEvent);
                        _logger.LogDebug("Event appended to stream for operation {operation} on {table}", changeEvent.Operation.ToString(), table_name);
                    }
                }
                if (changesFound)
                {
                    getNextLsn.Parameters["@lastLsn"].Value = fromLsn;
                    using (var rdr = getNextLsn.ExecuteReader())
                    {
                        while (rdr.Read())
                            rdr.GetBytes(0, 0, fromLsn, 0, 10);
                    }
                    updateLsn.Parameters["@lsn"].Value = fromLsn;
                    updateLsn.ExecuteNonQuery();

                }
            }
            catch (SqlException ex) when (ex.Number == 313)
            { 
                // expected
                _logger.LogDebug("SQL313: FromLSN={fromLsn}, ToLSN={toLsn}", Convert.ToHexString(fromLsn), Convert.ToHexString(toLsn));
            }

            await Task.Delay(500, _cancellationToken);
        }
        await cn.CloseAsync();
        cn.Dispose();
        _changeStream.OnCompleted();

        _logger.LogInformation("SQL Server CDC monitoring for table {table} in schema {schema} stopped.", table_name, schema_name);
    }
}
