using MilestoneTG.ChangeStream.Server;

var builder = WebApplication.CreateBuilder(args);
builder.Services.AddControllers();
builder.Services.AddHostedService<Propagator>();
var app = builder.Build();

app.MapControllers();

app.Run();
