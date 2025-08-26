#	Use a Microsoft image with .NET core runtime (https://mcr.microsoft.com/en-us/product/dotnet/aspnet/about)
FROM mcr.microsoft.com/dotnet/aspnet:9.0 AS final

#	Set the working directory
WORKDIR /work

#	Copy package
COPY . .

#	Run console app
CMD ["sh", "-c", "dotnet EventHubConsole.dll -l $sourceLevel -a $authentication -f $fqdn -e $eventHub -d $dbUri -t $templateName -r $recordsPerPayload -b $batchSize -p $parallelPartitions --throughput-target $throughputTarget -c $isOutputCompressed"]

#--event-hub-connection-string $eventHubConnectionString 