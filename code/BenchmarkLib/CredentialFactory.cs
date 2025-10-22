using Azure.Core;
using Azure.Identity;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BenchmarkLib
{
    public static class CredentialFactory
    {
        public static TokenCredential CreateCredentials(string authentication)
        {
            if (string.IsNullOrWhiteSpace(authentication)
                || string.Compare(authentication.Trim(), "azcli", true) == 0)
            {
                return new AzureCliCredential();
                //return new DefaultAzureCredential();
            }
            else
            {
                return new ManagedIdentityCredential();
            }
        }
    }
}