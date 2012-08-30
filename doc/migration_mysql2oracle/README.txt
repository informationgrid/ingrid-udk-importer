
Migration IGC 3.2.0 von MySQL nach Oracle
=========================================

Basis-Migration via SQL Developer
---------------------------------
Download SQL Developer z.B. hier:
http://www.oracle.com/technetwork/developer-tools/sql-developer/downloads/index.html?ssSourceSiteId=ocomen

Migration Dokumentation s. 
http://docs.oracle.com/cd/E18464_01/doc.30/e17472.pdf
s. Chapter 2

Basis vor Migration:
- (Initiale XE Verbindung angelegt mit User "System")
- Neuen Benutzer MIGRATIONS/MIGRATIONS angelegt (XE -> Andere Benutzer)
- Neue Verbindung Migration_Repository@XE angelegt mit Benutzer MIGRATIONS
- MySQL Verbindung angelegt (Treiber z.B.: mysql-connector-java-5.1.21)

Migration:
- Rechtsklick auf Datenbank in MySql Verbindung -> "Zu Oracle migrieren ..."
- In Wizard:
	- Als Zieldatenbank/-verbindung auch Repository Verbindung wählen
	- zunächst Daten offline migrieren (schreibt Scripts)
- Nach Migration neue Verbindung anlegen zu migrierter Datenbank, username ist der Datenbankname, ACHTUNG: passwd ist lowercase !
	- bei Migration Datenbank igc_test_lgv ist USER / PASSWD z.B. IGC_TEST_LGV / igc_test_lgv
- Dann via Migrationsprojekt Daten zu neuer Verbindung migrieren (Rechtsklick auf "Konvertierte Datenbankobjekte" und "Daten verschieben...")


Danach fix Datentypen spezieller Columns
----------------------------------------
Skript
migration_igc3.2.0_postSQLDeveloperMigration.sql
ausführen.
