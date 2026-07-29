#!/bin/sh

set -- dotnet EventHubExperimentConsole.dll

if [ -n "${sourceLevel:-}" ]; then
  set -- "$@" -l "$sourceLevel"
fi

if [ -n "${authentication:-}" ]; then
  set -- "$@" -a "$authentication"
fi

if [ -n "${configUri:-}" ]; then
  set -- "$@" -c "$configUri"
fi

exec "$@"