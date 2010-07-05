
Syslisten
=========
Nach dem Import/Upgrade der UDK-Daten auf die IGC Version 1.0.3 müssen die Syslisten aktualisiert
werden, um vor der IGC Version 1.0.4 einen konsistenten Zustand herbei zu führen.
Ab IGC Version 1.0.4 können die Syslisten im IGE editiert/exportiert/importiert werden, sind dann
also individuell pflegbar !
ACHTUNG: Es existieren allgemeingültige und länderspezifische Syslisten, weshalb die Syslisten NICHT
einheitlich vom Importer geschrieben werden, sondern per csv-Dateien in den entsprechenden IGC Katalog
importiert werden müssen.

Import csv-Dateien in IGC Katalog:
---------------------------------

Um einen korrekten Syslisten Zustand nach dem Upgrade auf die version 1.0.3 zu gewährleisten, folgendes Vorgehen:
1. sys_list Tabelle leeren
2. globale länderübergreifende Syslisten importieren (sys_list_global.csv)
3. lokale länderspezifische Syslisten importieren (z.B. sys_list_local_ni.csv).

OPTIONAL:
Mit dem Skript delete_global_sys_list.sql werden nur die globalen Syslisten gelöscht, falls nötig.
ACHTUNG: Beim Import der globalen Syslisten muß IMMER gewährleistet sein, dass die IDs 1-1000 noch nicht vergeben sind !


Import csv via phpMyAdmin (MySQL):
----------------------------------

- Tabelle "sys_list" auswählen und Tab "Leeren"

- Tabelle "sys_list" auswählen und Tab "Importieren"
	csv-Datei auswählen
Von Default abweichend:
	Anzahl der am Anfang zu überspringenden Einträge (Abfragen): 1
Default:
	Zeichencodierung der Datei: utf8

	Tabelleninhalt ersetzen: nein
	Ignoriere doppelte Zeilen: nein
	Felder getrennt mit: ;
	Felder eingeschlossen von: "
	Felder escaped von: \
	Zeilen getrennt mit: auto
	Spaltennamen:


Import Oracle:
--------------

- csv Dateien u.U. nach ANSI konvertieren, um Sonderzeichen korrekt zu übernehmen
