# covid-data
Project extracts EU and UK covid data from
- add links

data is then transformed and loaded into a reporting schema


### Initial Startup issues
If you have issues getting the container to run check the postgres container log and se if the foolowing line is present

``/usr/local/bin/docker-entrypoint.sh: line ...: /docker-entrypoint-initdb.d/init-database.sh: cannot execute: required file not found``

If it is, open /config/init-database.sh in a text exitoe and check the line feed character it should be ``LF`` but can sometome get update to ``CRLF`` fi ti has then change it back and save the file. Yo wiul lalso need ot delete.db folder to re-initialise the database, once you restart the container the dtaabse should be create successfult and ther container wil start.
