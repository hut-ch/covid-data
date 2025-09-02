# Troubleshooting

## Initial Startup issues
If you have issues getting the container to run check the postgres container log and see if the foolowing line is present.

``/usr/local/bin/docker-entrypoint.sh: line ...: /docker-entrypoint-initdb.d/init-database.sh: cannot execute: required file not found``

This appears to happensometrhing =on windows where the line feed wil be change from the unix ``LF`` to Windows ``CRLF`` which cause in init script to not be detected correctly.

Steps to resolve
- Open /config/init-database.sh in a text editor (VS Code or Notepad++ will work) and check the line feed character it should be ``LF`` but can sometome get update to ``CRLF``
- In both VS Code and Notepad++ check in the bottom right and look for either ``CRLF`` or ``LF`` if you click it you will be prometed to select a new value.  
![notpad++](/docs/img/line-feed-issue.png)
- Ensure it is set to ``LF`` and save the file.
- You will also need to delete the ``.db`` folder to re-initialise the database
- Once you restart the container the database should be created successfully and the container will start.


## Resetting the Project
