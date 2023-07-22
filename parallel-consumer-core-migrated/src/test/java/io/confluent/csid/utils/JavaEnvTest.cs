using System;
using Xunit;
using Xunit.Abstractions;

namespace io.confluent.csid.utils
{
    public class JavaEnvTest
    {
        private readonly ITestOutputHelper _output;

        public JavaEnvTest(ITestOutputHelper output)
        {
            _output = output;
        }

        /**
         * Used to manually inspect the java environment at runtime - particularly useful for CI environments
         */
        [Fact]
        public void CheckJavaEnvironment()
        {
            _output.WriteLine($"Java all env: {Pretty(System.getProperties().entrySet())}");
        }

        private string Pretty(object obj)
        {
            // Implement the logic to pretty print the object
            throw new NotImplementedException();
        }
    }
}