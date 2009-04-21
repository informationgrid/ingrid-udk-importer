
INSPIRE Themes
==============
- inspire_themes.csv
hier auch noch mal als csv Datei, WERDEN ABER VOM IMPORTER IN DER VERSION 1.0.4 IN DEN KATALOG GESCHRIEBEN ! also kein csv Import n�tig

Syslisten
=========

- ISO 639-2 Language Code List - Codes for the representation of names of languages (Library of Congress).doc
Komplett Sprachliste von Kst aus email "L�nder und Sprachlisten", 17.04.2009 10:29.
Als Basis f�r die entsprechende Sysliste im IGC.


- sys_list_erw.csv
urspr�ngliche Gesamt-Syslist Datei aus NI, diese enth�lt jedoch l�nderspezifische Syslisten (z.B. Rechtsgrundlagen 1350)
- sys_list_erw_nur_local.csv
NI spezifische Syslisten, s. email von unten

- sys_list_global.csv
allgemeine Syslisten, die f�r alle L�nder gleich sind ! wurde von Kst separiert, s. Email Kst: "AW: WG: Inhalte im NRW-UDK" vom 2.12.08, 14:53
- sys_list_local_*
l�nderspezifische Syslisten. Erzeugt indem jeweiliger UDK5 Katalog komplett importiert wurde (von Cash Export) und die globalen Syslisten gel�scht wurden. Das Resultat wurde nach diesen csv exportiert.
ACHTUNG: wenn n�tig wurden die IDs im csv dann per Excel angepasst, damit sie nicht mit den globalen Ids (aus sys_list_erw_nur_global.csv) kollidieren. D.h. die lokalen IDs 
m�ssen >= 1000 sein, wenn dies nicht der Fall war, wurden die IDs angepasst, so dass sie ab 1000 beginnen !

SQL Skripte
-----------
- delete_global_sys_list.sql
l�scht alle globalen syslisten aus der sys_list Tabelle. �brig bleiben die lokalen Syslisten. Wurde benutzt um lokale Syslist csv Dateien zu erstellen.

Import in IGC Katalog:
======================

VORSICHT: Es existieren l�nderspezifische Syslisten, s.o.
Um einen korrekten Zustand herbei zu f�hren, folgendes Vorgehen:
1. sys_list Tabelle leeren
2. lokale l�nderspezifische Syslisten importieren (z.B. sys_list_local_ni.csv). ACHTUNG: IDs m�ssen > 1000 sein, damit sie nicht mit globalen IDs kollidieren !
3. globale l�nder�bergreifende Syslisten importieren mit "Tabelleninhalt ersetzen: NEIN" (sys_list_erw_nur_global.csv)


Import csv via phpMyAdmin:
--------------------------
mit phpMyAdmin in sys_list Tabelle importieren !
-> Tabelle "sys_list" ausw�hlen und Tab "Importieren"

Von Default abweichend:
	Anzahl der am Anfang zu �berspringenden Eintr�ge (Abfragen): 1
	Tabelleninhalt ersetzen: ja/nein (ACHTUNG: Ersetzen funktioniert auf cash nicht ! VORHER TABELLE LEEREN !!!)

Default:
	Zeichencodierung der Datei: utf8

	Ignoriere doppelte Zeilen: nein
	Felder getrennt mit: ;
	Felder eingeschlossen von: "
	Felder escaped von: \
	Zeilen getrennt mit: auto
	Spaltennamen:


Original von Kst geschickte Datei, s email: "AW: sys_list zur �bersetzung" vom 26.09.2008 13:44
Fehler wurden gefixt, s. Historie in svn.