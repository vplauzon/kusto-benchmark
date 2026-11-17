# kusto-benchmark
Tools to run a benchmark

## Continuous builds

The [continuous-build workflow](.github/workflows/continuous-build.yaml) builds
and publishes one component per run. In GitHub Actions, select **Run workflow**
and choose `experiment` (the default), `event-hub`, `ingestor`, or `query`.
All four components are available for manual builds; pushes to `main` that
change `code/**` continue to build only Experiment.

Each build publishes a versioned image and updates its `dev:latest` tag in the
configured Azure Container Registry, using the `dev` environment. Version patches
use the workflow run number; the consolidated workflow starts a new numbering
sequence rather than continuing any of the previous workflows' sequences.
