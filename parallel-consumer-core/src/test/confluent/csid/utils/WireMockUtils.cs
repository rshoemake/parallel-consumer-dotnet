using WireMock.Net.StandAlone;
using WireMock.RequestBuilders;
using WireMock.ResponseBuilders;

namespace Confluent.Csid.Utils
{
    public class WireMockUtils
    {
        public const string StubResponse = "Good times.";

        public WireMockServer SetupWireMock()
        {
            var stubServer = WireMockServer.Start();
            stubServer.Given(Request.Create().WithPath("/"))
                .RespondWith(Response.Create().WithBody(StubResponse));
            stubServer.Given(Request.Create().WithPath("/api"))
                .RespondWith(Response.Create().WithBody(StubResponse));
            return stubServer;
        }
    }
}