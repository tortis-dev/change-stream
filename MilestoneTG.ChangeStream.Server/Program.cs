using MilestoneTG.ChangeStream;
using MilestoneTG.ChangeStream.Server;
using MilestoneTG.ChangeStream.Server.SqlServer;

var builder = WebApplication.CreateBuilder(args);
builder.Services.AddControllers();
builder.Services.AddHostedService<CdcServer>();
builder.Services.AddTransient<SqlServerChangeSource>();
builder.Services.AddSingleton<IConnectionStringFactory, AppSettingsConnectionStringFactory>();
builder.Services.AddOptions();
builder.Services.Configure<CdcSettings>(builder.Configuration.GetSection("cdc")); 
var app = builder.Build();

app.MapControllers();

app.Run();
