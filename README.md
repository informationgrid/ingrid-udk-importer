UDK Importer
============

This software is part of the InGrid software package. The UDK Importer (German: **U**mwelt **D**aten **K**atalog or Engish InGrid Catalog) is part of the IGE iPlug and handles all database update related functionality (alter database structure, alter default database content). It can run also stand alone to update a database manually.


Requirements
-------------

- a InGrid Catalog database

Installation
------------

Download from https://dev.informationgrid.eu/ingrid-distributions/ingrid-udk-importer/
 
or

build from source with `mvn package assembly:single`.

Execute

```
java -jar ingrid-udk-importer-x.x.x-installer.jar
```

and follow the install instructions.

Obtain further information at https://dev.informationgrid.eu/


Contribute
----------

- Issue Tracker: https://github.com/informationgrid/ingrid-udk-importer/issues
- Source Code: https://github.com/informationgrid/ingrid-udk-importer
 
### Set up eclipse project

```
mvn eclipse:eclipse
```

and import project into eclipse.

Support
-------

If you are having issues, please let us know: info@informationgrid.eu

License
-------

The project is licensed under the EUPL license.
