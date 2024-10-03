using BenchmarkLib;

namespace QueryConsole
{
    internal class Program
    {
        public static string AssemblyVersion
        {
            get
            {
                var version = typeof(Program).Assembly.GetName().Version;
                var versionText = version == null
                    ? "<VERSION MISSING>"
                    : version.ToString();

                return versionText;
            }
        }

        static void Main(string[] args)
        {
            Console.WriteLine();
            Console.WriteLine($"Kusto Query Console {AssemblyVersion}");
            Console.WriteLine();
            Console.WriteLine($"Command line:  {string.Join(" ", args)}");
        }
    }
}