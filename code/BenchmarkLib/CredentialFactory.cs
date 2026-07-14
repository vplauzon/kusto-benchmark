using Azure.Core;
using Azure.Identity;
using System;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace BenchmarkLib
{
    public static class CredentialFactory
    {
        public static async Task<TokenCredential> CreateCredentialsAsync(string authentication)
        {
            if (string.IsNullOrWhiteSpace(authentication)
                || string.Compare(authentication.Trim(), "azcli", true) == 0)
            {
                return new AzureCliCredential();
                //return new DefaultAzureCredential();
            }
            else
            {
                var credentials = new ManagedIdentityCredential(new ManagedIdentityCredentialOptions());

                await PrintManagedIdentityDebugInfoAsync(credentials);

                return credentials;
            }
        }

        private static async Task PrintManagedIdentityDebugInfoAsync(TokenCredential credentials)
        {
            try
            {
                var tokenContext = new TokenRequestContext(new[] { "https://management.azure.com/.default" });
                var token = await credentials.GetTokenAsync(tokenContext, CancellationToken.None);
                var tokenParts = token.Token.Split('.');

                if (tokenParts.Length < 2)
                {
                    Console.WriteLine("ManagedIdentity debug: unexpected token format.");
                    return;
                }

                var payloadJson = DecodeJwtSegment(tokenParts[1]);
                Console.WriteLine($"ManagedIdentity token payload: {payloadJson}");

                using var payloadDoc = JsonDocument.Parse(payloadJson);
                var payloadRoot = payloadDoc.RootElement;

                payloadRoot.TryGetProperty("oid", out var oid);
                payloadRoot.TryGetProperty("appid", out var appid);
                payloadRoot.TryGetProperty("sub", out var sub);

                Console.WriteLine(
                    $"ManagedIdentity principal ids => oid:{oid.GetString() ?? "<null>"}, appid:{appid.GetString() ?? "<null>"}, sub:{sub.GetString() ?? "<null>"}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"ManagedIdentity debug failed: {ex}");
            }
        }

        private static string DecodeJwtSegment(string segment)
        {
            var base64 = segment
                .Replace('-', '+')
                .Replace('_', '/');

            var padding = base64.Length % 4;
            if (padding > 0)
            {
                base64 = base64.PadRight(base64.Length + (4 - padding), '=');
            }

            var bytes = Convert.FromBase64String(base64);
            return Encoding.UTF8.GetString(bytes);
        }
    }
}