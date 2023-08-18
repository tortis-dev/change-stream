namespace MilestoneTG.ChangeStream;

public interface IChangeSource
{
    IObservable<ChangeEvent> ChangeStream { get; }

    void StartObserving();

    void StopObserving();
}