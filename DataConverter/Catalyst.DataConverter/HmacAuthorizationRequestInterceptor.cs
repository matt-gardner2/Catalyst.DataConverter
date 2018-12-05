using System;

namespace DataConverter
{
    using System.Linq;
    using System.Net.Http;
    using System.Security.Cryptography;
    using System.Text;

    using Fabric.Databus.Interfaces.Http;

    public class HmacAuthorizationRequestInterceptor : IHttpRequestInterceptor
    {
        private readonly string appId;

        private readonly string secret;

        public HmacAuthorizationRequestInterceptor(string appId, string secret)
        {
            this.appId = appId;
            this.secret = secret;
        }

        public void InterceptRequest(HttpMethod method, HttpRequestMessage request)
        {
            this.AddHmacRequestHeaders(method, request);
        }

        private void AddHmacRequestHeaders(HttpMethod method, HttpRequestMessage request)
        {
            string authHeader = null;
            DateTime requestDt = DateTime.Now.ToUniversalTime();
            request.Headers.Date = requestDt;
            if (method == HttpMethod.Post)
            {
                MD5 md5 = MD5.Create();
                string postData = request.Properties.Single(prop => prop.Key == "application/json").Value.ToString();
                var contentMd5 = this.GetMd5(md5, postData);
                
                if (!string.IsNullOrEmpty(contentMd5))
                {
                    request.Headers.Add("Content-MD5", contentMd5);
                    authHeader = this.GetAuthHeader(
                        "application/json",
                        contentMd5,
                        request.RequestUri.PathAndQuery,
                        requestDt);
                }
            }
            else
            {
                authHeader = this.GetAuthHeader(string.Empty, string.Empty, request.RequestUri.PathAndQuery, requestDt);
            }

            request.Headers.Add("Authorization", $"APIAuth {authHeader}");
        }

        private string GetMd5(MD5 md5Hash, string input)
        {
            // Convert the input string to a byte array and compute the hash. 
            byte[] hash = md5Hash.ComputeHash(Encoding.UTF8.GetBytes(input));

            return Convert.ToBase64String(hash);
        }

        private string GetAuthHeader(string requestContentType, string requestBodyMd5, string requestUrl, DateTime requestTs)
        {
            // 1. Build canonical string
            var canonicalString = this.GetCanonicalString(requestContentType, requestBodyMd5, requestUrl, requestTs);
            
            // 2. Encrypt canonical string
            var encryptedCanonicalString = this.GetHmac(canonicalString);

            // 3. Create Auth Header
            var finalizedAuthHeader = this.GetHeader(this.appId, encryptedCanonicalString);


            return finalizedAuthHeader;
        }

        private string GetCanonicalString(string contentType, string bodyMd5, string url, DateTime timestamp)
        {
            var timestampFormatted = $"{timestamp:R}"; 
            return $"{contentType},{bodyMd5},{url},{timestampFormatted}";
        }

        private string GetHmac(string value)
        {
            return this.GetHmac(Encoding.UTF8.GetBytes(value));
        }

        private string GetHmac(byte[] valueBytes)
        {
            if (string.IsNullOrEmpty(this.secret))
            {
                throw new ArgumentException("Expected private key not found!");
            }
            if (valueBytes == null)
            {
                throw new ArgumentException("Expected value to encrypt not found!");
            }

            var secretBytes = Convert.FromBase64String(this.secret);
            string signature;

            using (var hmac = new HMACSHA1(secretBytes))
            {
                var hash = hmac.ComputeHash(valueBytes);
                signature = Convert.ToBase64String(hash);
            }

            return signature.Replace("\n", "\\n");
        }

        private string GetHeader(string appId, string hmacSignature)
        {
            if (string.IsNullOrEmpty(appId))
            {
                throw new ArgumentException("Missing required parameter: appId");
            }

            if (string.IsNullOrEmpty(hmacSignature))
            {
                throw new ArgumentException("Missing required parameter: hMacSignature");
            }

            return $"{appId}:{hmacSignature}";
        }
    }
}
