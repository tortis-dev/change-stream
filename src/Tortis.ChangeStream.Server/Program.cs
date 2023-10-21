using Tortis.ChangeStream;
using Serilog;

var builder = WebApplication.CreateBuilder(args);

var logger = new LoggerConfiguration()
    .ReadFrom.Configuration(builder.Configuration)
    .CreateLogger();
builder.Logging.ClearProviders().AddSerilog(logger);

builder.Services.AddChangeStream(builder.Configuration);

var app = builder.Build();

app.Run();

Log.CloseAndFlush();
