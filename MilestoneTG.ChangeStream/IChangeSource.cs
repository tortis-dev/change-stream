namespace MilestoneTG.ChangeStream;

public interface IChangeSource
{
    IObservable<ChangeEvent> ChangeStream { get; }

    void Configure(SourceSettings settings);

    void StartObserving();

    void StopObserving();
}