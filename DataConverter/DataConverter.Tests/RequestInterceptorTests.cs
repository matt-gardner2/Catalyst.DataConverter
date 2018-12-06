using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Diagnostics.Contracts;
using System.IO;
using System.Reflection;

namespace UnitTestProject1
{
    using System.Collections.Generic;
    using System.Data;
    using System.Data.Common;
    using System.Net.Http;

    using DataConverter;

    using Fabric.Databus.Config;
    using Fabric.Databus.Interfaces.Http;
    using System.Data.SQLite;
    using System.Diagnostics;
    using System.Linq;
    using System.Net;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;

    using Fabric.Databus.Domain.ProgressMonitors;
    using Fabric.Databus.Http;
    using Fabric.Databus.Interfaces.FileWriters;
    using Fabric.Databus.Interfaces.Loggers;
    using Fabric.Databus.Interfaces.Sql;
    using Fabric.Databus.PipelineRunner;
    using Fabric.Databus.Shared.Loggers;
    using Fabric.Databus.SqlGenerator;

    using Moq;
    using Moq.Protected;

    using Newtonsoft.Json.Linq;

    using Unity;

    [TestClass]
    public class RequestInterceptorTests
    {
        [TestMethod]
        public void TestMethod1()
        {
            var fileContents = "<?xml version=\"1.0\" encoding=\"utf-8\" ?><Job xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:noNamespaceSchemaLocation=\"..\\..\\Fabric.Databus.Config\\Fabric.Databus.Config.xsd\"><Config><ConnectionString>Data Source=:memory:</ConnectionString><Url>https://stg.api.neutrino.upmce.net/api/v1/patient_document/5?user[root]=UserRoot&amp;user[extension]=UserExtension&amp;tid=healthcatalyst_phi</Url><LocalSaveFolder>foo</LocalSaveFolder><UseMultipleThreads>false</UseMultipleThreads><UrlUserName>myUser</UrlUserName><UrlPassword>myPassword</UrlPassword><UrlMethod>Get</UrlMethod></Config><Data><TopLevelDataSource Key=\"TextID\" Path=\"$\" Name=\"First\" TableOrView=\"Text\"></TopLevelDataSource></Data></Job>";
            var config = new ConfigReader().ReadXmlFromText(fileContents);
            config.Config.UrlMethod = HttpMethod.Get;
            string connectionString = "Data Source=:memory:";
            using (var connection = new SQLiteConnection(connectionString))
            {
                connection.Open();
                string sql = "CREATE TABLE Text (TextID varchar(64), PatientID int, TextTXT varchar(255))";

                SQLiteCommand command = connection.CreateCommand();
                command.CommandText = sql;
                command.ExecuteNonQuery();

                sql = "INSERT INTO Text (TextID, PatientID, TextTXT) values ('1', 9001, 'This is my first note')";

                command.CommandText = sql;
                command.ExecuteNonQuery();

                sql = @";WITH CTE AS ( SELECT
Text.*,Text.[TextID] AS [KeyLevel1]
FROM Text
 )  SELECT * from CTE LIMIT 1";
                command.CommandText = sql;
                command.ExecuteNonQuery();
                using (var progressMonitor = new ProgressMonitor(new TestConsoleProgressLogger()))
                {
                    using (var cancellationTokenSource = new CancellationTokenSource())
                    {
                        var container = new UnityContainer();
                        container.RegisterInstance<IProgressMonitor>(progressMonitor);
                        container.RegisterInstance<ISqlConnectionFactory>(
                            new SqlLiteConnectionFactory(new SqlLiteConnectionWrapper(connection)));

                        var integrationTestFileWriter = new IntegrationTestFileWriter { IsWritingEnabled = true };
                        container.RegisterInstance<IFileWriter>(integrationTestFileWriter);
                        container.RegisterInstance<ITemporaryFileWriter>(integrationTestFileWriter);

                        container.RegisterType<ISqlGeneratorFactory, SqlLiteGeneratorFactory>();
                        var mockRepository = new MockRepository(MockBehavior.Strict);
                        JObject expectedJson = new JObject(
                            new JProperty("TextID", "1"),
                            new JProperty("PatientID", 9001),
                            new JProperty("TextTXT", "This is my first note"));
                        var handlerMock = new Mock<HttpMessageHandler>(MockBehavior.Strict);
                        handlerMock
                            .Protected()
                            .Setup<Task<HttpResponseMessage>>(
                                "SendAsync",
                                ItExpr.IsAny<HttpRequestMessage>(),
                                ItExpr.IsAny<CancellationToken>())
                            .Callback<HttpRequestMessage, CancellationToken>((request, token) =>
                                {
                                    var content = request.Content.ReadAsStringAsync().Result;
                                    Assert.IsTrue(JToken.DeepEquals(expectedJson, JObject.Parse(content)), content);

                                    Assert.AreEqual("APIAuth", request.Headers.Authorization.Scheme);
                                    var actualParameter = request.Headers.Authorization.Parameter;

                                    var expectedByteArray = Encoding.ASCII.GetBytes($"{config.Config.UrlUserName}:{config.Config.UrlPassword}");
                                    var expectedParameter = Convert.ToBase64String(expectedByteArray);

                                    // Assert.AreEqual(expectedParameter, actualParameter);
                                })
                            .ReturnsAsync(new HttpResponseMessage(HttpStatusCode.OK)
                                              {
                                                  Content = new StringContent(string.Empty)
                                              })
                            .Verifiable();
                        var expectedUri = new Uri("https://stg.api.neutrino.upmce.net/api/v1/patient_document/5?user[root]=UserRoot&user[extension]=UserExtension&tid=healthcatalyst_phi");

                        var mockHttpClientFactory = mockRepository.Create<IHttpClientFactory>();
                        mockHttpClientFactory.Setup(service => service.Create())
                            .Returns(new HttpClient(handlerMock.Object));

                        container.RegisterInstance(mockHttpClientFactory.Object);
                        var appId = "REDACTED";
                        var appSecret = "REDACTED";
                        var tenantId = "REDACTED";
                        var tenantSecret = "REDACTED";
                        var interceptor = new HmacAuthorizationRequestInterceptor(appId, appSecret, tenantId, tenantSecret);
                        //var basicAuthorizationRequestInterceptor = new BasicAuthorizationRequestInterceptor(
                        //    config.Config.UrlUserName,
                        //    config.Config.UrlPassword);

                        var mockHttpRequestInterceptor = mockRepository.Create<IHttpRequestInterceptor>();
                        mockHttpRequestInterceptor.Setup(
                                service => service.InterceptRequest(It.IsAny<HttpMethod>(), It.IsAny<HttpRequestMessage>()))
                            .Callback<HttpMethod, HttpRequestMessage>(
                                (method, message) =>
                                    {
                                        interceptor.InterceptRequest(method, message);
                                    })
                            .Verifiable();


                        container.RegisterInstance(mockHttpRequestInterceptor.Object);

                        var mockHttpResponseInterceptor = mockRepository.Create<IHttpResponseInterceptor>();
                        mockHttpResponseInterceptor.Setup(
                                service => service.InterceptResponse(
                                    HttpMethod.Get,
                                    expectedUri,
                                    It.IsAny<string>(),
                                    HttpStatusCode.OK,
                                    string.Empty,
                                    It.IsAny<long>()))
                            .Verifiable();

                        container.RegisterInstance(mockHttpResponseInterceptor.Object);

                        // Act
                        var stopwatch = new Stopwatch();
                        stopwatch.Start();

                        var pipelineRunner = new PipelineRunner(container, cancellationTokenSource.Token);

                        pipelineRunner.RunPipeline(config);

                        // Assert
                        Assert.AreEqual(1, integrationTestFileWriter.Count);

                        var expectedPath = integrationTestFileWriter.CombinePath(config.Config.LocalSaveFolder, "1.json");
                        Assert.IsTrue(integrationTestFileWriter.ContainsFile(expectedPath));

                        Assert.IsTrue(
                            JToken.DeepEquals(
                                expectedJson,
                                JObject.Parse(integrationTestFileWriter.GetContents(expectedPath))));

                        handlerMock.Protected()
                            .Verify(
                                "SendAsync",
                                Times.Exactly(1),
                                ItExpr.Is<HttpRequestMessage>(
                                    req => req.Method == HttpMethod.Get
                                           && req.RequestUri == expectedUri),
                                ItExpr.IsAny<CancellationToken>());

                        mockHttpRequestInterceptor.Verify(
                            i => i.InterceptRequest(HttpMethod.Get, It.IsAny<HttpRequestMessage>()),
                            Times.Once);

                        mockHttpResponseInterceptor.Verify(
                            service => service.InterceptResponse(
                                HttpMethod.Get,
                                expectedUri,
                                It.IsAny<string>(),
                                HttpStatusCode.OK,
                                string.Empty,
                                It.IsAny<long>()),
                            Times.Once);
                        stopwatch.Stop();
                    }

                    connection.Close();
                }
            }


            //interceptor.InterceptRequest(HttpMethod.Post, );
        }
    }

    #region imported classes
    static class TestFileLoader
    {
        [Pure]
        internal static string GetFileContents(string folder, string sampleFile)
        {
            var asm = Assembly.GetExecutingAssembly();
            var assemblyName = asm.GetName().Name;
            var resource = $"{assemblyName}.{folder}.{sampleFile}";
            using (var stream = asm.GetManifestResourceStream(resource))
            {
                if (stream != null)
                {
                    var reader = new StreamReader(stream);
                    return reader.ReadToEnd();
                }
            }
            return string.Empty;
        }
    }
    public class SqlLiteConnectionFactory : ISqlConnectionFactory
    {
        /// <summary>
        /// The connection.
        /// </summary>
        private readonly SqlLiteConnectionWrapper connection;

        /// <inheritdoc />
        public SqlLiteConnectionFactory(SqlLiteConnectionWrapper connection)
        {
            this.connection = connection;
        }

        /// <inheritdoc />
        public DbConnection GetConnection(string connectionString)
        {
            return this.connection;
        }
    }
    public class SqlLiteConnectionWrapper : DbConnection
    {
        /// <summary>
        /// The connection.
        /// </summary>
        private readonly DbConnection connection;

        /// <summary>
        /// Initializes a new instance of the <see cref="SqlLiteConnectionWrapper"/> class.
        /// </summary>
        /// <param name="connection">
        /// The connection.
        /// </param>
        public SqlLiteConnectionWrapper(DbConnection connection)
        {
            this.connection = connection;
        }

        /// <inheritdoc />
        public override string ConnectionString { get; set; }

        /// <inheritdoc />
        public override string Database => this.connection.Database;

        /// <inheritdoc />
        public override ConnectionState State => this.connection.State;

        /// <inheritdoc />
        public override string DataSource => this.connection.DataSource;

        /// <inheritdoc />
        public override string ServerVersion => this.connection.ServerVersion;

        /// <inheritdoc />
        public override void Close()
        {
            // don't close connection since we'll do it
        }

        /// <inheritdoc />
        public override void ChangeDatabase(string databaseName)
        {
            throw new NotImplementedException();
        }

        /// <inheritdoc />
        public override void Open()
        {
            // do nothing since the connection is already open
        }

        /// <inheritdoc />
        protected override DbCommand CreateDbCommand()
        {
            return this.connection.CreateCommand();
        }

        /// <inheritdoc />
        protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
        {
            throw new NotImplementedException();
        }
    }

    public class IntegrationTestFileWriter : IFileWriter, ITemporaryFileWriter
    {
        /// <summary>
        /// The files.
        /// </summary>
        private readonly Dictionary<string, string> files = new Dictionary<string, string>();

        /// <inheritdoc />
        public bool IsWritingEnabled { get; set; }

        /// <summary>
        /// The count.
        /// </summary>
        public int Count => this.files.Count;

        /// <inheritdoc />
        public void CreateDirectory(string path)
        {
            // no need to do anything
        }

        /// <inheritdoc />
        public Task WriteToFileAsync(string filepath, string text)
        {
            if (!this.files.ContainsKey(filepath))
            {
                this.files.Add(filepath, text);
            }
            else
            {
                this.files[filepath] = text;
            }

            return Task.CompletedTask;
        }

        /// <summary>
        /// The open stream for writing.
        /// </summary>
        /// <param name="filepath">
        /// The filepath.
        /// </param>
        /// <returns>
        /// The <see cref="Stream"/>.
        /// </returns>
        public Stream OpenStreamForWriting(string filepath)
        {
            return new IntegrationTestFileStream(this, filepath);
        }

        /// <inheritdoc />
        public Stream CreateFile(string path)
        {
            return this.OpenStreamForWriting(path);
        }

        /// <inheritdoc />
        public Task WriteStreamAsync(string path, Stream stream)
        {
            if (stream.CanSeek)
            {
                stream.Seek(0, SeekOrigin.Begin);
            }

            using (var s = new StreamReader(stream))
            {
                var text = s.ReadToEnd();
                return this.WriteToFileAsync(path, text);
            }
        }

        /// <inheritdoc />
        public void DeleteDirectory(string folder)
        {
            // do nothing
        }

        /// <inheritdoc />
        public string CombinePath(string folder, string file)
        {
            return $"{folder}|{file}";
        }

        /// <summary>
        /// The contains file.
        /// </summary>
        /// <param name="expectedPath">
        /// The expected path.
        /// </param>
        /// <returns>
        /// The <see cref="bool"/>.
        /// </returns>
        public bool ContainsFile(string expectedPath)
        {
            return this.files.ContainsKey(expectedPath);
        }

        /// <summary>
        /// The get contents.
        /// </summary>
        /// <param name="expectedPath">
        /// The expected path.
        /// </param>
        /// <returns>
        /// The <see cref="string"/>.
        /// </returns>
        public string GetContents(string expectedPath)
        {
            return this.files[expectedPath];
        }
    }
    public class IntegrationTestFileStream : Stream
    {
        /// <summary>
        /// The writer.
        /// </summary>
        private readonly IntegrationTestFileWriter writer;

        /// <summary>
        /// The filepath.
        /// </summary>
        private readonly string filepath;

        /// <summary>
        /// The stream implementation.
        /// </summary>
        private readonly Stream streamImplementation;

        /// <summary>
        /// Initializes a new instance of the <see cref="IntegrationTestFileStream"/> class.
        /// </summary>
        /// <param name="writer">
        /// The writer.
        /// </param>
        /// <param name="filepath">
        /// file path</param>
        public IntegrationTestFileStream(IntegrationTestFileWriter writer, string filepath)
        {
            this.writer = writer;
            this.filepath = filepath;
            this.streamImplementation = new MemoryStream();
        }

        /// <inheritdoc />
        public override bool CanRead => this.streamImplementation.CanRead;

        /// <inheritdoc />
        public override bool CanSeek => this.streamImplementation.CanSeek;

        /// <inheritdoc />
        public override bool CanWrite => this.streamImplementation.CanWrite;

        /// <inheritdoc />
        public override long Length => this.streamImplementation.Length;

        /// <inheritdoc />
        public override long Position
        {
            get => this.streamImplementation.Position;
            set => this.streamImplementation.Position = value;
        }

        /// <inheritdoc />
        public override void Flush()
        {
            this.streamImplementation.Flush();
            this.writer.WriteStreamAsync(this.filepath, this.streamImplementation);
        }

        /// <inheritdoc />
        public override long Seek(long offset, SeekOrigin origin)
        {
            return this.streamImplementation.Seek(offset, origin);
        }

        /// <inheritdoc />
        public override void SetLength(long value)
        {
            this.streamImplementation.SetLength(value);
        }

        /// <inheritdoc />
        public override int Read(byte[] buffer, int offset, int count)
        {
            return this.streamImplementation.Read(buffer, offset, count);
        }

        /// <inheritdoc />
        public override void Write(byte[] buffer, int offset, int count)
        {
            this.streamImplementation.Write(buffer, offset, count);
        }
    }
    public class SqlLiteGeneratorFactory : ISqlGeneratorFactory
    {
        /// <inheritdoc />
        public ISqlGenerator Create()
        {
            return new SqlLiteGenerator();
        }
    }
    public class SqlLiteGenerator : AbstractBaseSqlGenerator
    {
        /// <inheritdoc />
        protected override void AppendSelectStatement(StringBuilder sb, string destinationEntity)
        {
            sb.AppendLine("SELECT");
            if (!this.SelectColumns.Any())
            {
                this.AddColumn("*");
            }

            var columnList = this.GetColumnList();

            sb.AppendLine(columnList);
            sb.AppendLine($"FROM {destinationEntity}");

            if (this.RangeFilter != null)
            {
                sb.AppendLine(
                    $"WHERE [{this.RangeFilter.ColumnName}] BETWEEN {this.RangeFilter.StartVariable} AND {this.RangeFilter.EndVariable}");
            }

            if (this.OrderByColumnAscending != null)
            {
                sb.AppendLine($"ORDER BY [{this.OrderByColumnAscending}] ASC");
            }
            if (this.TopFilterCount > 0)
            {
                sb.AppendLine($"LIMIT {this.TopFilterCount}");
            }
        }

    }
    #endregion

}
