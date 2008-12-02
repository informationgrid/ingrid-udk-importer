
Syslisten
=========
- sys_list_erw.csv
ursprüngliche Gesamt-Syslist Datei aus NI, diese enthält jedoch länderspezifische Syslisten (z.B. Rechtsgrundlagen 1350)
- sys_list_erw_nur_global.csv
allgemeine Syslisten, die für alle Länder gleich sind ! wurde von Kst separiert, s. Email Kst: "AW: WG: Inhalte im NRW-UDK" vom 2.12.08, 14:53
- sys_list_erw_nur_local.csv
NI spezifische Syslisten, s. email von oben
- sys_list_local_*
länderspezifische Syslisten. Erzeugt indem jeweiliger UDK5 Katalog komplett importiert wurde (von Cash Export) und die globalen Syslisten gelöscht wurden. Das Resultat wurde nach diesen csv exportiert.


Import in IGC Katalog:
======================

VORSICHT: Es existieren länderspezifische Syslisten, s.o.
Um einen korrekten Zustand herbei zu führen, folgendes Vorgehen:
1. sys_list Tabelle leeren
2. lokale länderspezifische Syslisten importieren (z.B. sys_list_local_ni.csv)
3. globale länderübergreifende Syslisten importieren mit "Tabelleninhalt ersetzen: NEIN" (sys_list_erw_nur_global.csv)


SQL Skripte
-----------
- delete_global_sys_list.sql
löscht alle globalen syslisten aus der sys_list Tabelle. Übrig bleiben die lokalen Syslisten. Wurde benutzt um lokale Syslist csv Dateien zu erstellen.


Import csv via phpMyAdmin:
--------------------------
mit phpMyAdmin in sys_list Tabelle importieren !
-> Tabelle "sys_list" auswählen und Tab "Importieren"

Von Default abweichend:
	Anzahl der am Anfang zu überspringenden Einträge (Abfragen): 1
	Tabelleninhalt ersetzen: ja/nein (ACHTUNG: Ersetzen funktioniert auf cash nicht ! VORHER TABELLE LEEREN !!!)

Default:
	Zeichencodierung der Datei: utf8

	Ignoriere doppelte Zeilen: nein
	Felder getrennt mit: ;
	Felder eingeschlossen von: "
	Felder escaped von: \
	Zeilen getrennt mit: auto
	Spaltennamen:


Original von Kst geschickte Datei, s email: "AW: sys_list zur Übersetzung" vom 26.09.2008 13:44
Fehler wurden gefixt, s. Historie in svn.