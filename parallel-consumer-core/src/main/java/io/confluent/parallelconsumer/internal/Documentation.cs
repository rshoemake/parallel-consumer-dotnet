namespace Confluent.ParallelConsumer.Internal
{
    public static class Documentation
    {
        public const string DOCS_ROOT = "https://github.com/confluentinc/parallel-consumer/";

        public static string GetLinkHtmlToDocSection(string section)
        {
            return DOCS_ROOT + section;
        }
    }
}