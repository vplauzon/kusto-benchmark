#	Use a Microsoft image with .NET core runtime (https://mcr.microsoft.com/en-us/product/dotnet/aspnet/about)
FROM mcr.microsoft.com/dotnet/aspnet:8.0 AS final

#	Set the working directory
WORKDIR /work

#	Copy package
COPY . .

#	Run console app
CMD ["sh", "-c", "dotnet QueryConsole.dll -d $dbUri -i $ingestionTable -t $templateName -s $blobSize -a $authentication -l $sourceLevel -m $mapping -p $parallelStream"]
