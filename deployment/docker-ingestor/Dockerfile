#	Use a Microsoft image with .NET core runtime (https://mcr.microsoft.com/en-us/product/dotnet/aspnet/about)
FROM mcr.microsoft.com/dotnet/aspnet:8.0 AS final

#	Set the working directory
WORKDIR /work

#	Copy package
COPY . .

#	Run console app
CMD ["sh", "-c", "dotnet IngestorConsole.dll -d $dbUri -i $ingestionTable -t $templateName -r $rowCount -a $authentication -l $sourceLevel -m $mapping -p $parallelStream"]
