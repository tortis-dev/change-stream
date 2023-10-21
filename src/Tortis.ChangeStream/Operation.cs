// ReSharper disable InconsistentNaming
namespace Tortis.ChangeStream;

[PublicAPI]
public enum Operation
{
    DELETE = 1,
    CREATE = 2,
    OLD    = 3,
    UPDATE = 4
}