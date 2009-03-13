package de.ingrid.importer.udk.util;

import java.util.HashMap;
import java.util.LinkedHashMap;

public class InspireThemesHelper {

	// INSPIRE THEMES
	// --------------
	static public int noInspireThemeId = 99999;

	// german syslist
	static public LinkedHashMap<Integer, String> inspireThemes_de = new LinkedHashMap<Integer, String>();
	static {
		inspireThemes_de.put(101, "Koordinatenreferenzsysteme");
		inspireThemes_de.put(102, "Geografische Gittersysteme");
		inspireThemes_de.put(103, "Geografische Bezeichnungen");
		inspireThemes_de.put(104, "Verwaltungseinheiten");
		inspireThemes_de.put(105, "Adressen");
		inspireThemes_de.put(106, "Flurstücke/Grundstücke (Katasterparzellen)");
		inspireThemes_de.put(107, "Verkehrsnetze");
		inspireThemes_de.put(108, "Gewässernetz");
		inspireThemes_de.put(109, "Schutzgebiete");
		inspireThemes_de.put(201, "Höhe");
		inspireThemes_de.put(202, "Bodenbedeckung");
		inspireThemes_de.put(203, "Orthofotografie");
		inspireThemes_de.put(204, "Geologie");
		inspireThemes_de.put(301, "Statistische Einheiten");
		inspireThemes_de.put(302, "Gebäude");
		inspireThemes_de.put(303, "Boden");
		inspireThemes_de.put(304, "Bodennutzung");
		inspireThemes_de.put(305, "Gesundheit und Sicherheit");
		inspireThemes_de.put(306, "Versorgungswirtschaft und staatliche Dienste");
		inspireThemes_de.put(307, "Umweltüberwachung");
		inspireThemes_de.put(308, "Produktions- und Industrieanlagen");
		inspireThemes_de.put(309, "Landwirtschaftliche Anlagen und Aquakulturanlagen");
		inspireThemes_de.put(310, "Verteilung der Bevölkerung — Demografie");
		inspireThemes_de.put(311, "Bewirtschaftungsgebiete/Schutzgebiete/geregelte Gebiete und Berichterstattungseinheiten");
		inspireThemes_de.put(312, "Gebiete mit naturbedingten Risiken");
		inspireThemes_de.put(313, "Atmosphärische Bedingungen");
		inspireThemes_de.put(314, "Meteorologisch-geografische Kennwerte");
		inspireThemes_de.put(315, "Ozeanografisch-geografische Kennwerte");
		inspireThemes_de.put(316, "Meeresregionen");
		inspireThemes_de.put(317, "Biogeografische Regionen");
		inspireThemes_de.put(318, "Lebensräume und Biotope");
		inspireThemes_de.put(319, "Verteilung der Arten");
		inspireThemes_de.put(320, "Energiequellen");
		inspireThemes_de.put(321, "Mineralische Bodenschätze");
		inspireThemes_de.put(noInspireThemeId, "Kein INSPIRE-Thema");
	}
	// german "searchterm" to theme id. LOWERCASE because of comparison
	static public HashMap<String, Integer[]> termToInspireIds_de = new HashMap<String, Integer[]>(); 
	static {
		// TODO: adapt to KST values !!!

		termToInspireIds_de.put("koordinate", new Integer[]{101});
		termToInspireIds_de.put("referenzsystem", new Integer[]{101});
		termToInspireIds_de.put("gittersystem", new Integer[]{102});
		termToInspireIds_de.put("geografische bezeichnung", new Integer[]{103});
		termToInspireIds_de.put("verwaltungseinheit", new Integer[]{104});
		termToInspireIds_de.put("adresse", new Integer[]{105});
		termToInspireIds_de.put("flurstück", new Integer[]{106});
		termToInspireIds_de.put("grundstück", new Integer[]{106});
		termToInspireIds_de.put("katasterparzelle", new Integer[]{106});
		termToInspireIds_de.put("verkehr", new Integer[]{107});
		termToInspireIds_de.put("gewässer", new Integer[]{108});
		termToInspireIds_de.put("schutzgebiet", new Integer[]{109, 311});
		termToInspireIds_de.put("höhe", new Integer[]{201});
		termToInspireIds_de.put("bodenbedeckung", new Integer[]{202});
		termToInspireIds_de.put("orthofotografie", new Integer[]{203});
		termToInspireIds_de.put("geologie", new Integer[]{204});
		termToInspireIds_de.put("statistische einheit", new Integer[]{301});
		termToInspireIds_de.put("gebäude", new Integer[]{302});
		termToInspireIds_de.put("boden", new Integer[]{303});
		termToInspireIds_de.put("bodennutzung", new Integer[]{304});
		termToInspireIds_de.put("gesundheit", new Integer[]{305});
		termToInspireIds_de.put("sicherheit", new Integer[]{305});
		termToInspireIds_de.put("versorgungswirtschaft", new Integer[]{306});
		termToInspireIds_de.put("staatliche dienste", new Integer[]{306});
		termToInspireIds_de.put("umweltüberwachung", new Integer[]{307});
		termToInspireIds_de.put("produktion", new Integer[]{308});
		termToInspireIds_de.put("industrieanlage", new Integer[]{308});
		termToInspireIds_de.put("landwirtschaft", new Integer[]{309});
		termToInspireIds_de.put("aquakultur", new Integer[]{309});
		termToInspireIds_de.put("bevölkerung", new Integer[]{310});
		termToInspireIds_de.put("demografie", new Integer[]{310});
		termToInspireIds_de.put("bewirtschaftungsgebiet", new Integer[]{311});
		termToInspireIds_de.put("geregelte gebiet", new Integer[]{311});
		termToInspireIds_de.put("geregeltes gebiet", new Integer[]{311});
		termToInspireIds_de.put("berichterstattungseinheit", new Integer[]{311});
		termToInspireIds_de.put("naturbedingte risiken", new Integer[]{312});
		termToInspireIds_de.put("atmosphäre", new Integer[]{313});
		termToInspireIds_de.put("meteorologie", new Integer[]{314});
		termToInspireIds_de.put("ozeanografie", new Integer[]{315});
		termToInspireIds_de.put("meeresregion", new Integer[]{316});
		termToInspireIds_de.put("biogeografische region", new Integer[]{317});
		termToInspireIds_de.put("lebensräume", new Integer[]{318});
		termToInspireIds_de.put("lebensraum", new Integer[]{318});
		termToInspireIds_de.put("biotop", new Integer[]{318});
		termToInspireIds_de.put("verteilung der arten", new Integer[]{319});
		termToInspireIds_de.put("energiequelle", new Integer[]{320});
		termToInspireIds_de.put("mineral", new Integer[]{321});
		termToInspireIds_de.put("bodenschätze", new Integer[]{321});
		termToInspireIds_de.put("bodenschatz", new Integer[]{321});
	}

	
	// english syslist
	static public LinkedHashMap<Integer, String> inspireThemes_en = new LinkedHashMap<Integer, String>(); 
	static {
		inspireThemes_en.put(101, "Coordinate reference systems");
		inspireThemes_en.put(102, "Geographical grid systems");
		inspireThemes_en.put(103, "Geographical names");
		inspireThemes_en.put(104, "Administrative units");
		inspireThemes_en.put(105, "Addresses");
		inspireThemes_en.put(106, "Cadastral parcels");
		inspireThemes_en.put(107, "Transport networks");
		inspireThemes_en.put(108, "Hydrography");
		inspireThemes_en.put(109, "Protected sites");
		inspireThemes_en.put(201, "Elevation");
		inspireThemes_en.put(202, "Land cover");
		inspireThemes_en.put(203, "Orthoimagery");
		inspireThemes_en.put(204, "Geology");
		inspireThemes_en.put(301, "Statistical units");
		inspireThemes_en.put(302, "Buildings");
		inspireThemes_en.put(303, "Soil");
		inspireThemes_en.put(304, "Land use");
		inspireThemes_en.put(305, "Human health and safety");
		inspireThemes_en.put(306, "Utility and governmental services");
		inspireThemes_en.put(307, "Environmental monitoring facilities");
		inspireThemes_en.put(308, "Production and industrial facilities");
		inspireThemes_en.put(309, "Agricultural and aquaculture facilities");
		inspireThemes_en.put(310, "Population distribution — demography");
		inspireThemes_en.put(311, "Area management/restriction/regulation zones and reporting units");
		inspireThemes_en.put(312, "Natural risk zones");
		inspireThemes_en.put(313, "Atmospheric conditions");
		inspireThemes_en.put(314, "Meteorological geographical features");
		inspireThemes_en.put(315, "Oceanographic geographical features");
		inspireThemes_en.put(316, "Sea regions");
		inspireThemes_en.put(317, "Bio-geographical regions");
		inspireThemes_en.put(318, "Habitats and biotopes");
		inspireThemes_en.put(319, "Species distribution");
		inspireThemes_en.put(320, "Energy resources");
		inspireThemes_en.put(321, "Mineral resources");
		inspireThemes_en.put(noInspireThemeId, "No INSPIRE Theme");
	}
}
