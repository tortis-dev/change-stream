using Tortis.ChangeStream;
using Serilog;

var builder = WebApplication.CreateBuilder(args);

var logger = new LoggerConfiguration()
    .ReadFrom.Configuration(builder.Configuration)
    .CreateLogger();
builder.Logging.ClearProviders().AddSerilog(logger);
builder.Services.AddHealthChecks();

builder.Services.AddChangeStream(builder.Configuration);

var app = builder.Build();

app.UseHealthChecks("/healthz");
app.Run();

Log.CloseAndFlush();
