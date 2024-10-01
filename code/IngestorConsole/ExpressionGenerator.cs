using System.Collections.Immutable;
using System.Reflection.Emit;
using System.Text;
using System.Text.RegularExpressions;
using static System.Net.Mime.MediaTypeNames;

namespace IngestorConsole
{
    internal class ExpressionGenerator
    {
        #region Inner types
        private record TemplateReplacement(int Index, int Length, Func<string> Generator);

        private record ReferenceValueReplacement(
            int Index,
            int Length,
            string TableName,
            string GroupName);
        #endregion

        private static readonly Random _random = new();
        private readonly IImmutableList<Func<string>> _generators;

        #region Constructors
        private ExpressionGenerator(IEnumerable<Func<string>> generators)
        {
            _generators = generators.ToImmutableArray();
        }

        internal static async Task<ExpressionGenerator> CreateAsync(
            string template,
            KustoClient engineClient,
            CancellationToken ct)
        {
            var generators = await CompileGeneratorsAsync(template, engineClient, ct);

            return new ExpressionGenerator(generators);
        }

        private static async Task<IEnumerable<Func<string>>> CompileGeneratorsAsync(
            string template,
            KustoClient engineClient,
            CancellationToken ct)
        {
            var timestampNowReplacements = ExtractTimestampNow(template);
            var referencedValueReplacements =
                await ExtractReferencedValueAsync(template, engineClient, ct);
            var generateIdsReplacements = ExtractGenerateId(template);
            var r = timestampNowReplacements
                .Concat(referencedValueReplacements)
                .Concat(generateIdsReplacements);

            return CompileGenerators(template, r);
        }

        private static IEnumerable<Func<string>> CompileGenerators(
            string template,
            IEnumerable<TemplateReplacement> replacements)
        {
            var sortedReplacements = replacements
                .OrderBy(g => g.Index)
                .ToImmutableArray();
            var index = 0;

            foreach (var replacement in sortedReplacements)
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
                    () => DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffff"));
                template = template.Substring(index + PATTERN.Length);
                index = template.IndexOf(PATTERN);
            }
        }

        private static async Task<IEnumerable<TemplateReplacement>> ExtractReferencedValueAsync(
            string template,
            KustoClient engineClient,
            CancellationToken ct)
        {
            var referenceValueReplacements = FindReferenceValues(template).ToImmutableArray();
            var tableToGroupToValueMap =
                await LoadValuesAsync(referenceValueReplacements, engineClient, ct);
            var templateReplacements = referenceValueReplacements
                .Select(r => new TemplateReplacement(
                    r.Index,
                    r.Length,
                    () => RandomPick(tableToGroupToValueMap[r.TableName][r.GroupName])));

            return templateReplacements;
        }

        private static IEnumerable<TemplateReplacement> ExtractGenerateId(string template)
        {
            var match = Regex.Match(template, @"GenerateId\(""([^""]*)"",\s*""([^""]*)"",\s*(\d+)\)");

            while (match.Success)
            {
                var prefix = match.Groups[1].Value;
                var suffix = match.Groups[2].Value;
                var cardinality = int.Parse(match.Groups[3].Value);

                yield return new TemplateReplacement(
                    match.Index,
                    match.Length,
                    () => $"{prefix}{_random.Next(cardinality):D5}{suffix}");
                match = match.NextMatch();
            }
        }

        private static string RandomPick(IImmutableList<string> list)
        {
            var index = _random.Next(list.Count);

            return list[index];
        }

        private static async Task<IImmutableDictionary<string, IImmutableDictionary<string, IImmutableList<string>>>> LoadValuesAsync(
            IEnumerable<ReferenceValueReplacement> referenceValueReplacements,
            KustoClient engineClient,
            CancellationToken ct)
        {
            var tables = referenceValueReplacements
                .GroupBy(r => r.TableName);
            var tableTasks = tables
                .Select(g => new
                {
                    TableName = g.Key,
                    Task = engineClient.LoadReferenceValuesAsync(g.Key, g.Select(i => i.GroupName), ct)
                })
                .ToImmutableArray();

            await Task.WhenAll(tableTasks.Select(t => t.Task));

            var map = tableTasks
                .Select(t => new
                {
                    t.TableName,
                    SubMap = t.Task.Result
                })
                .ToImmutableDictionary(o => o.TableName, o => o.SubMap);

            return map;
        }

        private static IEnumerable<ReferenceValueReplacement> FindReferenceValues(string template)
        {
            var match = Regex.Match(template, @"ReferenceValue\(([^,]+),\s*([^)]+)\)");

            while (match.Success)
            {
                yield return new ReferenceValueReplacement(
                    match.Index,
                    match.Length,
                    match.Groups[1].Value,
                    match.Groups[2].Value);
                match = match.NextMatch();
            }
        }
        #endregion
        #endregion

        public int GenerateExpression(TextWriter writer)
        {
#if DEBUG
            var text = string.Concat(_generators.Select(g => g()));
#endif
            var totalLength = 0;

            foreach (var generator in _generators)
            {
                var subExpression = generator();

                totalLength += subExpression.Length;
                writer.Write(subExpression);
            }
            writer.WriteLine();

            return ++totalLength;
        }
    }
}