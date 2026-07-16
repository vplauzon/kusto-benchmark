#	Use a Microsoft image with .NET core runtime (https://mcr.microsoft.com/en-us/product/dotnet/aspnet/about)
FROM mcr.microsoft.com/dotnet/aspnet:10.0 AS final

#	Set the working directory
WORKDIR /work

#	Copy package
COPY . .

#	Run console app
CMD ["sh", "-c", "dotnet EventHubConsole.dll -l \"${sourceLevel-warning}\" -a \"${authentication-}\" -f \"${fqdn-}\" -e \"${eventHub-}\" --event-hub-connection-string \"${eventHubConnectionString-}\" -d \"${dbUri-}\" -t \"${templateName-}\" -r \"${recordsPerPayload-5}\" --throughput-target \"${throughputTarget-100}\" --max-time-between-batches \"${maxTimeBetweenBatches}\" --max-batch-size \"${maxBatchSize}\""]

#--event-hub-connection-string $eventHubConnectionString 