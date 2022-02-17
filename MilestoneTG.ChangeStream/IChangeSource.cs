namespace MilestoneTG.ChangeStream;

public interface IChangeSource
{
    IDisposable Subscribe(Action<ChangeEvent> onNext);
}