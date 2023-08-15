using System.Reactive.Subjects;
using Microsoft.Extensions.Logging;

namespace MilestoneTG.ChangeStream;

public abstract class ChangeSourceBase<TChangeSource> : IChangeSource
{
    readonly ISubject<ChangeEvent> _changeStream = new Subject<ChangeEvent>();
    readonly CancellationTokenSource _cancellationTokenSource = new();
    
    Task? _cdcTask;
    
    protected ChangeSourceBase(IConnectionStringFactory connectionStringFactory, ILogger<TChangeSource> logger)
    {
        ConnectionStringFactory = connectionStringFactory;
        Logger = logger;
    }

    public IObservable<ChangeEvent> ChangeStream => _changeStream;
    
    protected ILogger<TChangeSource> Logger { get; }

    protected IConnectionStringFactory ConnectionStringFactory { get; }
    
    protected SourceSettings? Settings { get; private set; }

    /// <summary>
    /// Publishes the event to the ChangeStream.
    /// </summary>
    /// <param name="changeEvent"></param>
    protected void Publish(ChangeEvent changeEvent)
    {
        Logger.LogTrace("Publishing change event to stream.");
        _changeStream.OnNext(changeEvent);
    }

    /// <summary>
    /// Publishes the error to the ChangeStream.
    /// </summary>
    /// <param name="error"></param>
    protected void Error(Exception error)
    {
        Logger.LogDebug(error, "Publishing error to stream.");
        _changeStream.OnError(error);
    }

    /// <summary>
    /// Marks the stream complete. No more events will be published or read.
    /// </summary>
    protected void Complete()
    {
        Logger.LogTrace("Completing stream.");
        _changeStream.OnCompleted();
    }

    public void StartObserving()
    {
        _cdcTask = Task.Factory.StartNew(() => Worker(_cancellationTokenSource.Token), _cancellationTokenSource.Token);
    }

    public void StopObserving()
    {
        _cancellationTokenSource.Cancel();
    }

    public void Configure(SourceSettings settings)
    {
        Settings = settings;
    }

    protected abstract Task Worker(CancellationToken cancellationToken);
}