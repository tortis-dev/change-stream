using MilestoneTG.ChangeStream;
using Serilog;
using Serilog.Events;

var builder = WebApplication.CreateBuilder(args);

var logger = new LoggerConfiguration()
    .ReadFrom.Configuration(builder.Configuration)
    .WriteTo.Seq("http://localhost:5341")
    .WriteTo.Console(LogEventLevel.Information)
    .CreateLogger();
builder.Logging.ClearProviders().AddSerilog(logger);

builder.Services.AddOptions();

builder.Services.AddChangeStream(builder.Configuration);

var app = builder.Build();

app.Run();
