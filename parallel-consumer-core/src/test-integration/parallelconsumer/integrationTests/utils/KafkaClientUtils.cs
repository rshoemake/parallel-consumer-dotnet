using System;
using System.Threading;
using Xunit;
using Xunit.Abstractions;

namespace io.confluent.parallelconsumer.integrationTests.sanity
{
    public class ProgressBarTest
    {
        private readonly ITestOutputHelper _output;

        public ProgressBarTest(ITestOutputHelper output)
        {
            _output = output;
        }

        [Fact]
        [Obsolete("For reference sanity only")]
        public void Width()
        {
            using (var build = new ProgressBar(100, "Progress", ConsoleColor.Green, _output.WriteLine))
            {
                while (build.Current < build.Max)
                {
                    build.Tick();
                    Thread.Sleep(100);
                }
            }
        }
    }
}