using MilestoneTG.ChangeStream;
using MilestoneTG.ChangeStream.Server;
using Serilog;
using Serilog.Events;

var builder = WebApplication.CreateBuilder(args);

var logger = new LoggerConfiguration()
    .ReadFrom.Configuration(builder.Configuration)
    .WriteTo.Seq("http://localhost:5341")
    .WriteTo.Console(LogEventLevel.Information)
    .CreateLogger();
builder.Logging.ClearProviders().AddSerilog(logger);

builder.Services.AddControllers();
builder.Services.AddHostedService<CdcServer>();
builder.Services.AddSingleton<IConnectionStringFactory, AppSettingsConnectionStringFactory>();
builder.Services.AddOptions();
builder.Services.Configure<CdcSettings>(builder.Configuration.GetSection("cdc")); 
var app = builder.Build();

app.MapControllers();

app.Run();
