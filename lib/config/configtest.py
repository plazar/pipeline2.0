import configtypes

download = configtypes.ConfigList()
download.add_config("api_service_url", configtypes.StrConfig())
download.add_config("directory", configtypes.ReadWriteConfig())
download.add_config("numrestores", configtypes.IntConfig())

download.api_service_url = 'some random string'
download.directory = '/tmp'
download.numrestores = 2

print download.numrestores
x = download.directory
print "%s hooey %s" % (x, download.api_service_url)
