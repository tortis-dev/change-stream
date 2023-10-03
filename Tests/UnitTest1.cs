using System.Numerics;
using Xunit.Abstractions;

namespace Tests;

public class UnitTest1
{
    ITestOutputHelper _output;

    public UnitTest1(ITestOutputHelper output)
    {
        _output = output;
    }

    [Fact]
    public void Test1()
    {
        string EventIdFormat = new String('0', 50);
        var signByte = new[] { (byte)0x00 };
        var bytes = Enumerable.Repeat<byte>(0xFF, 20).Concat(signByte).ToArray();
        var big = new BigInteger(bytes);
        _output.WriteLine(big.ToString(EventIdFormat));
        _output.WriteLine("{0}", big.ToString("G").Length);
    }
}