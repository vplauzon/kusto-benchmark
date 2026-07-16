#	Use a Microsoft image with .NET core runtime (https://mcr.microsoft.com/en-us/product/dotnet/aspnet/about)
FROM mcr.microsoft.com/dotnet/aspnet:10.0 AS final

#	Set the working directory
WORKDIR /work

#	Copy package
COPY . .

RUN chmod +x /work/entrypoint.sh

#	Run console app
CMD ["./entrypoint.sh"]
