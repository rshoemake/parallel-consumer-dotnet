using System;
using System.Data;
using System.Threading;
using Confluent.Kafka;
using Dapper;
using Npgsql;
using Xunit;

namespace Confluent.ParallelConsumer.IntegrationTests
{
    public class DbTest : BrokerIntegrationTest<string, string>
    {
        protected static readonly PostgreSQLContainer dbc;

        static DbTest()
        {
            dbc = new PostgreSQLContainer()
                .WithReuse(true);
            dbc.Start();
        }

        private IDbConnection connection;

        [BeforeEach]
        public void FollowDbLogs()
        {
            if (Log.IsEnabled(LogLevel.Debug))
            {
                var logConsumer = new Slf4jLogConsumer(Log);
                dbc.FollowOutput(logConsumer);
            }
        }

        private static readonly object DbLock = new object();

        [SneakyThrows]
        [BeforeEach]
        public void SetupDatabase()
        {
            var connectionString = dbc.GetConnectionString();
            connection = new NpgsqlConnection(connectionString);
            connection.Open();

            // create if exists doesn't seem to be thread safe - something around postgres creating indexes causes a distinct exception
            Monitor.Enter(DbLock);
            connection.Execute(@"
                CREATE TABLE IF NOT EXISTS DATA(
                    ID SERIAL PRIMARY KEY NOT NULL,
                    KEY TEXT NOT NULL,
                    VALUE TEXT NOT NULL
                );");
            Monitor.Exit(DbLock);
        }

        [SneakyThrows]
        [Fact]
        public void TestDatabaseSetup()
        {
            Assert.True(dbc.IsRunning); // sanity

            SavePayload("a", "test");
        }

        [SneakyThrows]
        private void SavePayload(string key, string payload)
        {
            var query = "insert into data(key, value) values(@Key, @Value)";
            connection.Execute(query, new { Key = key, Value = payload });
        }
    }
}