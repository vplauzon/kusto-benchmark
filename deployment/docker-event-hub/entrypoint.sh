#!/bin/sh

set -- dotnet EventHubConsole.dll

if [ -n "${sourceLevel:-}" ]; then
  set -- "$@" -l "$sourceLevel"
fi

if [ -n "${authentication:-}" ]; then
  set -- "$@" -a "$authentication"
fi

if [ -n "${fqdn:-}" ]; then
  set -- "$@" -f "$fqdn"
fi

if [ -n "${eventHub:-}" ]; then
  set -- "$@" -e "$eventHub"
fi

if [ -n "${eventHubConnectionString:-}" ]; then
  set -- "$@" --event-hub-connection-string "$eventHubConnectionString"
fi

if [ -n "${dbUri:-}" ]; then
  set -- "$@" -d "$dbUri"
fi

if [ -n "${templateName:-}" ]; then
  set -- "$@" -t "$templateName"
fi

if [ -n "${recordsPerPayload:-}" ]; then
  set -- "$@" -r "$recordsPerPayload"
fi

if [ -n "${throughputTarget:-}" ]; then
  set -- "$@" --throughput-target "$throughputTarget"
fi

if [ -n "${maxTimeBetweenBatches:-}" ]; then
  set -- "$@" --max-time-between-batches "$maxTimeBetweenBatches"
fi

if [ -n "${maxBatchSize:-}" ]; then
  set -- "$@" --max-batch-size "$maxBatchSize"
fi

exec "$@"
