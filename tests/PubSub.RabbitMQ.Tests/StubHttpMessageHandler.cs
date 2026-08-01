using System.Net;

namespace PubSub.RabbitMQ.Tests;

/// <summary>
/// Test double for <see cref="HttpMessageHandler"/>: records the last request and returns a
/// caller-supplied response (or throws a supplied exception to simulate a transport failure).
/// </summary>
internal sealed class StubHttpMessageHandler(Func<HttpRequestMessage, HttpResponseMessage> responder)
    : HttpMessageHandler
{
    public HttpRequestMessage? LastRequest { get; private set; }

    /// <summary>Always returns 200 with <paramref name="json"/> as the body.</summary>
    public static StubHttpMessageHandler Json(string json) =>
        new(_ => new HttpResponseMessage(HttpStatusCode.OK)
        {
            Content = new StringContent(json, System.Text.Encoding.UTF8, "application/json"),
        });

    /// <summary>Always returns the given status code with an empty body.</summary>
    public static StubHttpMessageHandler Status(HttpStatusCode status) =>
        new(_ => new HttpResponseMessage(status));

    /// <summary>Always throws, simulating a connection failure.</summary>
    public static StubHttpMessageHandler Throws() =>
        new(_ => throw new HttpRequestException("simulated transport failure"));

    protected override Task<HttpResponseMessage> SendAsync(
        HttpRequestMessage request, CancellationToken cancellationToken)
    {
        LastRequest = request;
        try
        {
            return Task.FromResult(responder(request));
        }
        catch (Exception ex)
        {
            return Task.FromException<HttpResponseMessage>(ex);
        }
    }
}
