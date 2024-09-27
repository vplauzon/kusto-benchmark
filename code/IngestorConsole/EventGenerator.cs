using System.Collections.Immutable;
using System.Reflection.Emit;
using System.Text;
using System.Text.RegularExpressions;

namespace IngestorConsole
{
    internal class EventGenerator
    {
        #region Inner types
        private record TemplateReplacement(int Index, int Length, Func<string> Generator);
        #endregion

        private readonly IImmutableList<Func<string>> _generators;

        #region Constructors
        private EventGenerator(IEnumerable<Func<string>> generators)
        {
            _generators = generators.ToImmutableArray();
        }

        internal static async Task<EventGenerator> CreateAsync(
            string template,
            KustoClient engineClient,
            CancellationToken ct)
        {
            await Task.CompletedTask;

            var generators = CompileGenerators(template);

            return new EventGenerator(generators);
        }

        private static IEnumerable<Func<string>> CompileGenerators(string template)
        {
            var timestampNowReplacements = ExtractTimestampNow(template);
            var replacements = timestampNowReplacements
                .OrderBy(g => g.Index)
                .ToImmutableArray();
            var index = 0;

            foreach (var replacement in replacements)
            {
                if (replacement.Index != index)
                {
                    var text = template.Substring(index, replacement.Index - index);

                    yield return () => text;
                }
                yield return replacement.Generator;
                index = replacement.Index + replacement.Length;
            }
            if (index != template.Length)
            {
                var text = template.Substring(index);

                yield return () => text;
            }
        }

        #region Generators
        private static IEnumerable<TemplateReplacement> ExtractTimestampNow(string template)
        {
            const string PATTERN = "TimestampNow()";

            var index = template.IndexOf(PATTERN);

            while (index != -1)
            {
                yield return new TemplateReplacement(
                    index,
                    PATTERN.Length,
                    () => DateTime.UtcNow.ToString());
                template = template.Substring(index + PATTERN.Length);
                index = template.IndexOf(PATTERN);
            }
        }
        #endregion
        #endregion

        public void GenerateEvent(TextWriter writer)
        {
#if DEBUG
            var text = string.Concat(_generators.Select(g => g()));
#endif
            foreach(var generator in _generators)
            {
                writer.Write(generator());
            }
            writer.WriteLine();
        }
    }
}