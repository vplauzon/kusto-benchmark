using System.Text;

namespace IngestorConsole
{
    internal class EventGenerator
    {
        private readonly string _template;

        #region Constructors
        private EventGenerator(string template)
        {
            _template = template;
        }

        internal static async Task<EventGenerator> CreateAsync(
            string template,
            KustoClient engineClient,
            CancellationToken ct)
        {
            await Task.CompletedTask;

            return new EventGenerator(template);
        }
        #endregion

        public void GenerateEvent(TextWriter writer)
        {
            writer.WriteLine(_template);
        }
    }
}