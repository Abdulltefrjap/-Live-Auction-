var builder = WebApplication.CreateBuilder(args);

// Add services to the container.
builder.Services.AddControllersWithViews();

// Register AuctionService so WebSocket handler can use it
builder.Services.AddSingleton<WebApplication4.Services.AuctionService>();

var app = builder.Build();

// Configure the HTTP request pipeline.
if (!app.Environment.IsDevelopment())
{
    app.UseExceptionHandler("/Home/Error");
    // The default HSTS value is 30 days. You may want to change this for production scenarios, see https://aka.ms/aspnetcore-hsts.
    app.UseHsts();
}

app.UseHttpsRedirection();
app.UseStaticFiles();
app.UseRouting();

app.UseWebSockets();

// WebSocket endpoint for live auction (async-safe sender, validation, and unsubscribe on close)
app.Map("/ws/auction", async context =>
{
    if (!context.WebSockets.IsWebSocketRequest)
    {
        context.Response.StatusCode = 400;
        return;
    }

    var auction = context.RequestServices.GetRequiredService<WebApplication4.Services.AuctionService>();
    using var ws = await context.WebSockets.AcceptWebSocketAsync();
    Console.WriteLine("WebSocket connected");

    // channel to queue outgoing messages for this socket
    var channel = System.Threading.Channels.Channel.CreateUnbounded<string>(new System.Threading.Channels.UnboundedChannelOptions { SingleReader = true, SingleWriter = false });
    var cts = CancellationTokenSource.CreateLinkedTokenSource(context.RequestAborted);

    // sender task drains the channel and writes to the socket asynchronously
    var senderTask = Task.Run(async () =>
    {
        try
        {
            await foreach (var payload in channel.Reader.ReadAllAsync(cts.Token))
            {
                try
                {
                    var bytes = System.Text.Encoding.UTF8.GetBytes(payload);
                    await ws.SendAsync(new ArraySegment<byte>(bytes), System.Net.WebSockets.WebSocketMessageType.Text, true, cts.Token);
                    Console.WriteLine($"Sent bid to client: {payload}");
                }
                catch (OperationCanceledException) { break; }
                catch (Exception ex) { Console.WriteLine($"Send error: {ex.Message}"); }
            }
        }
        catch (OperationCanceledException) { }
    }, cts.Token);

    // subscription handler enqueues serialized bids
    void OnBid(WebApplication4.Services.Bid b)
    {
        try
        {
            var payload = auction.SerializeBid(b);
            channel.Writer.TryWrite(payload);
        }
        catch (Exception ex) { Console.WriteLine($"OnBid error: {ex.Message}"); }
    }

    auction.Subscribe(OnBid);

    var buffer = new byte[1024 * 4];
    try
    {
        while (ws.State == System.Net.WebSockets.WebSocketState.Open && !cts.IsCancellationRequested)
        {
            var result = await ws.ReceiveAsync(new ArraySegment<byte>(buffer), cts.Token);
            if (result.MessageType == System.Net.WebSockets.WebSocketMessageType.Close)
            {
                break;
            }

            var msg = System.Text.Encoding.UTF8.GetString(buffer, 0, result.Count);
            Console.WriteLine($"Received WS message: {msg}");

            // Expect JSON: { "itemId": "1", "bidder": "Alice", "amount": 123.45 }
            try
            {
                using var doc = System.Text.Json.JsonDocument.Parse(msg);
                var root = doc.RootElement;
                var itemId = root.GetProperty("itemId").GetString() ?? "default";
                var bidder = root.GetProperty("bidder").GetString() ?? "anon";
                var amount = root.GetProperty("amount").GetDecimal();

                // basic validation
                if (string.IsNullOrWhiteSpace(itemId) || string.IsNullOrWhiteSpace(bidder) || amount <= 0)
                {
                    var err = System.Text.Json.JsonSerializer.Serialize(new { type = "error", message = "invalid bid" });
                    var bytes = System.Text.Encoding.UTF8.GetBytes(err);
                    await ws.SendAsync(new ArraySegment<byte>(bytes), System.Net.WebSockets.WebSocketMessageType.Text, true, cts.Token);
                    continue;
                }

                var prev = auction.GetHighest(itemId) ?? 0m;
                if (amount <= prev)
                {
                    var err = System.Text.Json.JsonSerializer.Serialize(new { type = "error", message = "bid too low", highest = prev });
                    var bytes = System.Text.Encoding.UTF8.GetBytes(err);
                    await ws.SendAsync(new ArraySegment<byte>(bytes), System.Net.WebSockets.WebSocketMessageType.Text, true, cts.Token);
                    continue;
                }

                // accept bid and broadcast
                auction.PlaceBid(itemId, bidder, amount);
            }
            catch (System.Text.Json.JsonException jex)
            {
                Console.WriteLine($"Error parsing WS message: {jex.Message}");
                var err = System.Text.Json.JsonSerializer.Serialize(new { type = "error", message = "bad json" });
                var bytes = System.Text.Encoding.UTF8.GetBytes(err);
                await ws.SendAsync(new ArraySegment<byte>(bytes), System.Net.WebSockets.WebSocketMessageType.Text, true, cts.Token);
            }
            catch (Exception ex) { Console.WriteLine($"Error handling WS message: {ex.Message}"); }
        }
    }
    catch (OperationCanceledException) { }
    finally
    {
        auction.Unsubscribe(OnBid);
        Console.WriteLine("WebSocket closing");
        try { channel.Writer.Complete(); } catch { }
        cts.Cancel();
        try { await senderTask; } catch { }
        await ws.CloseAsync(System.Net.WebSockets.WebSocketCloseStatus.NormalClosure, "closing", CancellationToken.None);
    }
});


app.UseAuthorization();

app.MapStaticAssets();

app.MapControllerRoute(
    name: "default",
    pattern: "{controller=Home}/{action=Index}/{id?}")
    .WithStaticAssets();


app.Run();
