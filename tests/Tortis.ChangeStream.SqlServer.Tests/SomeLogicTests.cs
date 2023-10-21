using System.Numerics;
using FluentAssertions;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.IdentityModel.Tokens;
using Xunit.Abstractions;

namespace Tortis.ChangeStream.SqlServer.Tests;

public class SomeLogicTests
{
    readonly ITestOutputHelper _outputHelper;

    public SomeLogicTests(ITestOutputHelper outputHelper)
    {
        _outputHelper = outputHelper;
    }

    /// <summary>
    /// Make sure unsigned behaves the way we think it does.
    /// </summary>
    [Fact]
    public void Unsigned()
    {
        // The max value for any LSN or SEQ is 10 bytes of FF.
        var bytes = Enumerable.Repeat((byte)0xFF, 10).ToArray();

        var signed = new BigInteger(bytes);
        var unsigned = new BigInteger(bytes, true);

        _outputHelper.WriteLine("{0:G}", signed);
        _outputHelper.WriteLine("{0:G}", unsigned);

        signed.Should().Be(-1);
        unsigned.Should().BeGreaterThan(1);
    }

    [Fact]
    public async Task Endianness()
    {
        var connectionStringFactory = new TestConnectionStringFactory();
        var cn = new SqlConnection(connectionStringFactory.GetConnectionString("test"));
        var cmd = new SqlCommand("insert into person (first_name, last_name) values ('Testy','McTest')", cn);

        await cn.OpenAsync();
        var tx = cn.BeginTransaction();
        cmd.Transaction = tx;
        for (var x = 0; x < 5; x++)
            await cmd.ExecuteNonQueryAsync();

        tx.Commit();

        await Task.Delay(2000);

        var changeCommand = new GetChangesCommand(cn, "dbo_person", new NullLoggerFactory());

        SqlDataReader? changes;
        do
        {
            changes = await changeCommand.ExecuteAsync(CancellationToken.None);
        } while (changes is null);

        var lsn = new byte[10];
        var seq = new byte[10];

        var lsns = new List<byte[]>();
        var seqs = new List<byte[]>();
        while (await changes.ReadAsync())
        {
            changes.GetBytes(0, 0, lsn, 0, 10);
            changes.GetBytes(1, 0, seq, 0, 10);

            lsns.Add(lsn.CloneByteArray());
            seqs.Add(seq.CloneByteArray());
        }

        // Least signifcant byte ↓↓
        //     0000009B00004DD00008
        // Unfortunately, when read from the reader into a byte array using rdr.GetBytes(),
        // 0000009B00004DD00008
        // ↑↑ This becomes the least significant byte (element 0).
        // So, to get the correct base10 value, we must reverse the array to make it little-endian.
        // 43628622695628808
        // You can test it here: https://www.rapidtables.com/convert/number/hex-to-decimal.html
        // BigInteger can handle big-endian, so we don't have to reverse the array ourselves and save on the allocation.
        for (var i = 0; i < lsns.Count; i++)
        {
            _outputHelper.WriteLine("{0}   {1}", string.Join("", lsns[i].Select(b => b.ToString("X2"))), string.Join("", seqs[i].Select(b => b.ToString("X2"))));
            _outputHelper.WriteLine("{0}   {1}", new BigInteger(lsns[i], true, true), new BigInteger(seqs[i], true, true));
        }
    }

    [Fact]
    public async Task TestSqlSource()
    {
        var connectionStringFactory = new TestConnectionStringFactory();
        var cn = new SqlConnection(connectionStringFactory.GetConnectionString("test"));
        var cmd = new SqlCommand("insert into person (first_name, last_name) values ('Testy','McTest')", cn);

        await cn.OpenAsync();
        var tx = cn.BeginTransaction();
        cmd.Transaction = tx;
        for (var x = 0; x < 5; x++)
            await cmd.ExecuteNonQueryAsync();

        tx.Commit();

        await Task.Delay(2000);

        var config = new Dictionary<string, object>
        {
            { "ConnectionStringName", "test" },
            { "SchemaName", "dbo" },
            { "TableName", "person" }
        };
        var sql = new SqlServerSource();
        sql.Configure(config, connectionStringFactory, new NullLoggerFactory());

        var eventList = new List<string>();

        do
        {
            await foreach (var e in sql.GetChanges(CancellationToken.None))
            {
                _outputHelper.WriteLine(e.EventId);
                eventList.Add(e.EventId);
            }
        } while (eventList.Count == 0);

        var sortedEventList = new string[eventList.Count];
        eventList.CopyTo(sortedEventList);
        Array.Sort(sortedEventList);
        sortedEventList.SequenceEqual(eventList).Should().BeTrue();

        _outputHelper.WriteLine("");
        foreach (var s in sortedEventList)
        {
            _outputHelper.WriteLine(s);
        }
    }
}

class TestConnectionStringFactory : IConnectionStringFactory
{
    public string GetConnectionString(string name)
    {
        var config = new ConfigurationBuilder()
            .AddUserSecrets<TestConnectionStringFactory>()
            .AddEnvironmentVariables()
            .Build();
        return config["TestSqlDb"];
    }
}
