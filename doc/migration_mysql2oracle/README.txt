
Migration 3.2.0 von MySQL nach Oracle
=====================================

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
	- Zieldatenbank: Als Verbindung auch Repository Verbindung wählen (mit "Zielobjekte löschen")
	- Daten verschieben: zunächst Daten "Offline" migrieren (mit "Daten leeren")
- Nach Migration NEUE VERBINDUNG anlegen zu migrierter Datenbank: username / password ist der Datenbankname -> ACHTUNG: User ist case insensitive / passwd ist lowercase !
	- bei Migration Datenbank igc_test_lgv ist USER / PASSWD z.B. IGC_TEST_LGV / igc_test_lgv
- Dann via Migrationsprojekt Daten zu neuer Verbindung migrieren
	- Rechtsklick auf "Konvertierte Datenbankobjekte" (in Ansicht "Migrationsprojekte") und "Daten verschieben..."
	- Modus: Online, Quelle: "MySQL", Ziel: neue Verbindung, "Daten leeren"

ACHTUNG:
Der Name der MySQL Datenbank sollte KEIN "-" enthalten, da dies zu Problemen führt.
Also vorher "ingrid-portal" umbenennen nach "ingrid_portal" !!!


Danach Fixes ausführen (z.B. fix Datentypen spezieller Columns ...)
-------------------------------------------------------------------
- Skripts ausführen:
	- IGC:		migration_igc3.2.0_fixes.sql
	- Portal:	migration_portal3.2.0_fixes.sql
	- mdek:		(kein Fix nötig)

ACHTUNG:
Im Portal werden alle Quartz Jobs gelöscht, da diese aus den persistenten Daten auf Oracle nicht erzeugt werden können (Fehler: EOF beim Lesen BLOB).
Die Default Jobs werden dann beim Starten des Portals wieder erzeugt.


Danach in Anwendungen Verbindung umstellen
------------------------------------------
- IGE iPlug in Datei conf\default-datasource.properties:
	# Example Oracle settings
	hibernate.driverClass=oracle.jdbc.OracleDriver
	hibernate.user=IGC_TEST_LGV
	hibernate.password=igc_test_lgv
	hibernate.dialect=org.hibernate.dialect.Oracle10gDialect
	hibernate.jdbcUrl=jdbc:oracle:thin:@localhost:1521:XE
- Portal, s. README_oracle.txt in apache-tomcat-...\conf\Catalina\localhost

	