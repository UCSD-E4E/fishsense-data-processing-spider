# Fishsense Data Processing Spider
This software package shall continuously crawl the data paths specified to collect, identify, and process dive data.

## Setup
1. Mount appropriate remote volumes:
```
docker volume create \
   --driver local \
   --opt type=cifs \
   --opt device=//storage.lan/media-movies \
   --opt o=addr=storage.lan,vers=3.0,username=foo,password=bar,uid=1000,gid=1000 \
   media-movies
```