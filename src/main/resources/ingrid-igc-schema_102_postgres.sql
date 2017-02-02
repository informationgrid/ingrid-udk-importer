CREATE TABLE t011_obj_serv_op_platform
(
	id BIGINT NOT NULL,
	version INTEGER NOT NULL DEFAULT 0,
	obj_serv_op_id BIGINT,
	line INTEGER DEFAULT 0,
	platform VARCHAR(120),
	PRIMARY KEY (id)
) 
;

CREATE INDEX idxOSerOPl_OSerOId ON t011_obj_serv_op_platform (obj_serv_op_id ASC);


CREATE TABLE t011_obj_serv_op_para
(
	id BIGINT NOT NULL,
	version INTEGER NOT NULL DEFAULT 0,
	obj_serv_op_id BIGINT,
	line INTEGER DEFAULT 0,
	name VARCHAR(120),
	direction VARCHAR(20),
	descr TEXT,
	optional INTEGER,
	repeatability INTEGER,
	PRIMARY KEY (id)
) 
;

CREATE INDEX idxOSerOPa_OSerOId ON t011_obj_serv_op_para (obj_serv_op_id ASC);


CREATE TABLE t011_obj_serv_op_depends
(
	id BIGINT NOT NULL,
	version INTEGER NOT NULL DEFAULT 0,
	obj_serv_op_id BIGINT,
	line INTEGER DEFAULT 0,
	depends_on VARCHAR(120),
	PRIMARY KEY (id)
) 
;

CREATE INDEX idxOserODe_OSerOId ON t011_obj_serv_op_depends (obj_serv_op_id ASC);


CREATE TABLE t011_obj_serv_op_connpoint
(
	id BIGINT NOT NULL,
	version INTEGER NOT NULL DEFAULT 0,
	obj_serv_op_id BIGINT,
	line INTEGER DEFAULT 0,
	connect_point VARCHAR(255),
	PRIMARY KEY (id)
) 
;

CREATE INDEX idxOSerOCP_OSerOId ON t011_obj_serv_op_connpoint (obj_serv_op_id ASC);


CREATE TABLE t011_obj_serv_version
(
	id BIGINT NOT NULL,
	version INTEGER NOT NULL DEFAULT 0,
	obj_serv_id BIGINT,
	line INTEGER DEFAULT 0,
	serv_version VARCHAR(80),
	PRIMARY KEY (id)
) 
;

CREATE INDEX idxOSerVers_OSerId ON t011_obj_serv_version (obj_serv_id ASC);


CREATE TABLE t011_obj_serv_operation
(
	id BIGINT NOT NULL,
	version INTEGER NOT NULL DEFAULT 0,
	obj_serv_id BIGINT,
	line INTEGER DEFAULT 0,
	name_key INTEGER,
	name_value VARCHAR(120),
	descr TEXT,
	invocation_name VARCHAR(255),
	PRIMARY KEY (id)
) 
;

CREATE INDEX idxOSerOper_OSerId ON t011_obj_serv_operation (obj_serv_id ASC);


CREATE TABLE t011_obj_geo_vector
(
	id BIGINT NOT NULL,
	version INTEGER NOT NULL DEFAULT 0,
	obj_geo_id BIGINT,
	line INTEGER DEFAULT 0,
	geometric_object_type INTEGER,
	geometric_object_count INTEGER,
	PRIMARY KEY (id)
) 
;

CREATE INDEX idxOGeoVect_OGeoId ON t011_obj_geo_vector (obj_geo_id ASC);


CREATE TABLE t011_obj_geo_symc
(
	id BIGINT NOT NULL,
	version INTEGER NOT NULL DEFAULT 0,
	obj_geo_id BIGINT,
	line INTEGER DEFAULT 0,
	symbol_cat_key INTEGER,
	symbol_cat_value VARCHAR(80),
	symbol_date VARCHAR(17),
	edition VARCHAR(80),
	PRIMARY KEY (id)
) 
;

CREATE INDEX idxOGeoSymc_OGeoId ON t011_obj_geo_symc (obj_geo_id ASC);


CREATE TABLE t011_obj_geo_supplinfo
(
	id BIGINT NOT NULL,
	version INTEGER NOT NULL DEFAULT 0,
	obj_geo_id BIGINT,
	line INTEGER DEFAULT 0,
	feature_type VARCHAR(255),
	PRIMARY KEY (id)
) 
;

CREATE INDEX idxOGeoSupp_OGeoId ON t011_obj_geo_supplinfo (obj_geo_id ASC);


CREATE TABLE t011_obj_geo_spatial_rep
(
	id BIGINT NOT NULL,
	version INTEGER NOT NULL DEFAULT 0,
	obj_geo_id BIGINT,
	line INTEGER DEFAULT 0,
	type INTEGER,
	PRIMARY KEY (id)
) 
;

CREATE INDEX idxOGeoSpat_OGeoId ON t011_obj_geo_spatial_rep (obj_geo_id ASC);


CREATE TABLE t011_obj_geo_scale
(
	id BIGINT NOT NULL,
	version INTEGER NOT NULL DEFAULT 0,
	obj_geo_id BIGINT,
	line INTEGER DEFAULT 0,
	scale INTEGER,
	resolution_ground DOUBLE PRECISION,
	resolution_scan DOUBLE PRECISION,
	PRIMARY KEY (id)
) 
;

CREATE INDEX idxOGeoScal_OGeoId ON t011_obj_geo_scale (obj_geo_id ASC);


CREATE TABLE t011_obj_geo_keyc
(
	id BIGINT NOT NULL,
	version INTEGER NOT NULL DEFAULT 0,
	obj_geo_id BIGINT,
	line INTEGER DEFAULT 0,
	keyc_key INTEGER,
	keyc_value VARCHAR(80),
	key_date VARCHAR(17),
	edition VARCHAR(80),
	PRIMARY KEY (id)
) 
;

CREATE INDEX idxOGeoKeyc_OGeoId ON t011_obj_geo_keyc (obj_geo_id ASC);


CREATE TABLE permission_obj
(
	id BIGINT NOT NULL,
	version INTEGER NOT NULL DEFAULT 0,
	uuid VARCHAR(40),
	permission_id BIGINT,
	idc_group_id BIGINT,
	PRIMARY KEY (id)
) 
;

CREATE INDEX IDXperm_obj_uuid ON permission_obj (uuid ASC);


CREATE TABLE object_reference
(
	id BIGINT NOT NULL,
	version INTEGER NOT NULL DEFAULT 0,
	obj_from_id BIGINT NOT NULL,
	obj_to_uuid VARCHAR(40) NOT NULL,
	line INTEGER DEFAULT 0,
	special_ref INTEGER DEFAULT 0,
	special_name VARCHAR(80),
	descr TEXT,
	PRIMARY KEY (id)
) 
;

CREATE INDEX idxObjRef_OFromId ON object_reference (obj_from_id ASC);


CREATE TABLE full_index_obj
(
	id BIGINT NOT NULL,
	version INTEGER NOT NULL DEFAULT 0,
	obj_node_id BIGINT NOT NULL,
	idx_name VARCHAR(255) NOT NULL,
	idx_value TEXT,
	PRIMARY KEY (id)
) 
;

CREATE INDEX idxObjIdxName ON full_index_obj (idx_name ASC);
CREATE INDEX idxFullObjNodedId ON full_index_obj (obj_node_id ASC);


CREATE TABLE t017_url_ref
(
	id BIGINT NOT NULL,
	version INTEGER NOT NULL DEFAULT 0,
	obj_id BIGINT,
	line INTEGER DEFAULT 0,
	url_link VARCHAR(255),
	special_ref INTEGER,
	special_name VARCHAR(80),
	content VARCHAR(255),
	datatype_key INTEGER,
	datatype_value VARCHAR(40),
	volume VARCHAR(20),
	icon VARCHAR(255),
	icon_text VARCHAR(80),
	descr TEXT,
	url_type INTEGER,
	PRIMARY KEY (id)
) 
;

CREATE INDEX idxUrlRef_ObjId ON t017_url_ref (obj_id ASC);


CREATE TABLE t015_legist
(
	id BIGINT NOT NULL,
	version INTEGER NOT NULL DEFAULT 0,
	obj_id BIGINT,
	line INTEGER DEFAULT 0,
	legist_value VARCHAR(120),
	legist_key INTEGER,
	PRIMARY KEY (id)
) 
;

CREATE INDEX idxLegis_ObjId ON t015_legist (obj_id ASC);


CREATE TABLE t014_info_impart
(
	id BIGINT NOT NULL,
	version INTEGER NOT NULL DEFAULT 0,
	obj_id BIGINT,
	line INTEGER DEFAULT 0,
	impart_value VARCHAR(80),
	impart_key INTEGER,
	PRIMARY KEY (id)
) 
;

CREATE INDEX idxInfImpart_ObjId ON t014_info_impart (obj_id ASC);


CREATE TABLE t0114_env_topic
(
	id BIGINT NOT NULL,
	version INTEGER NOT NULL DEFAULT 0,
	obj_id BIGINT NOT NULL,
	line INTEGER,
	topic_key INTEGER,
	PRIMARY KEY (id)
) 
;

CREATE INDEX idxEnvTop_ObjId ON t0114_env_topic (obj_id ASC);


CREATE TABLE t0114_env_category
(
	id BIGINT NOT NULL,
	version INTEGER NOT NULL DEFAULT 0,
	obj_id BIGINT NOT NULL,
	line INTEGER,
	cat_key INTEGER,
	PRIMARY KEY (id)
) 
;

CREATE INDEX idxEnvCat_obj_id ON t0114_env_category (obj_id ASC);


CREATE TABLE t0113_dataset_reference
(
	id BIGINT NOT NULL,
	version INTEGER NOT NULL DEFAULT 0,
	obj_id BIGINT,
	line INTEGER DEFAULT 0,
	reference_date VARCHAR(17),
	type INTEGER,
	PRIMARY KEY (id)
) 
;

CREATE INDEX idxDatRef_ObjId ON t0113_dataset_reference (obj_id ASC);


CREATE TABLE t0112_media_option
(
	id BIGINT NOT NULL,
	version INTEGER NOT NULL DEFAULT 0,
	obj_id BIGINT,
	line INTEGER DEFAULT 0,
	medium_note VARCHAR(255),
	medium_name INTEGER,
	transfer_size DOUBLE PRECISION,
	PRIMARY KEY (id)
) 
;

CREATE INDEX idxMediaOp_ObjId ON t0112_media_option (obj_id ASC);


CREATE TABLE t0110_avail_format
(
	id BIGINT NOT NULL,
	version INTEGER NOT NULL DEFAULT 0,
	obj_id BIGINT,
	line INTEGER DEFAULT 0,
	format_value VARCHAR(80),
	format_key INTEGER,
	ver VARCHAR(40),
	file_decompression_technique VARCHAR(80),
	specification VARCHAR(80),
	PRIMARY KEY (id)
) 
;

CREATE INDEX idxAvFormat_ObjId ON t0110_avail_format (obj_id ASC);


CREATE TABLE t011_obj_topic_cat
(
	id BIGINT NOT NULL,
	version INTEGER NOT NULL DEFAULT 0,
	obj_id BIGINT,
	line INTEGER DEFAULT 0,
	topic_category INTEGER,
	PRIMARY KEY (id)
) 
;


CREATE TABLE t011_obj_serv
(
	id BIGINT NOT NULL,
	version INTEGER NOT NULL DEFAULT 0,
	obj_id BIGINT,
	type_key INTEGER,
	type_value VARCHAR(255),
	history TEXT,
	environment TEXT,
	base VARCHAR(255),
	description TEXT,
	PRIMARY KEY (id)
) 
;

CREATE INDEX idxObjServ_ObjId ON t011_obj_serv (obj_id ASC);


CREATE TABLE t011_obj_project
(
	id BIGINT NOT NULL,
	version INTEGER NOT NULL DEFAULT 0,
	obj_id BIGINT,
	leader VARCHAR(80),
	member VARCHAR(255),
	description TEXT,
	PRIMARY KEY (id)
) 
;

CREATE INDEX idxOProj_ObjId ON t011_obj_project (obj_id ASC);


CREATE TABLE t011_obj_literature
(
	id BIGINT NOT NULL,
	version INTEGER NOT NULL DEFAULT 0,
	obj_id BIGINT,
	author VARCHAR(255),
	publisher VARCHAR(255),
	type_key INTEGER,
	type_value VARCHAR(80),
	publish_in VARCHAR(80),
	volume VARCHAR(40),
	sides VARCHAR(20),
	publish_year VARCHAR(20),
	publish_loc VARCHAR(80),
	loc VARCHAR(80),
	doc_info VARCHAR(255),
	base TEXT,
	isbn VARCHAR(40),
	publishing VARCHAR(80),
	description TEXT,
	PRIMARY KEY (id)
) 
;

CREATE INDEX idxOLit_ObjId ON t011_obj_literature (obj_id ASC);


CREATE TABLE t011_obj_geo
(
	id BIGINT NOT NULL,
	version INTEGER NOT NULL DEFAULT 0,
	obj_id BIGINT,
	special_base TEXT,
	data_base TEXT,
	method TEXT,
	referencesystem_value VARCHAR(255),
	referencesystem_key INTEGER,
	rec_exact DOUBLE PRECISION,
	rec_grade DOUBLE PRECISION,
	hierarchy_level INTEGER,
	vector_topology_level INTEGER,
	pos_accuracy_vertical DOUBLE PRECISION,
	keyc_incl_w_dataset INTEGER DEFAULT 0,
	PRIMARY KEY (id)
) 
;

CREATE INDEX idxOGeo_ObjId ON t011_obj_geo (obj_id ASC);


CREATE TABLE t011_obj_data_para
(
	id BIGINT NOT NULL,
	version INTEGER NOT NULL DEFAULT 0,
	obj_id BIGINT,
	line INTEGER,
	parameter VARCHAR(80),
	unit VARCHAR(120),
	PRIMARY KEY (id)
) 
;

CREATE INDEX idxODataPara_ObjId ON t011_obj_data_para (obj_id ASC);


CREATE TABLE t011_obj_data
(
	id BIGINT NOT NULL,
	version INTEGER NOT NULL DEFAULT 0,
	obj_id BIGINT,
	base VARCHAR(255),
	description TEXT,
	PRIMARY KEY (id)
) 
;

CREATE INDEX idxOData_ObjId ON t011_obj_data (obj_id ASC);


CREATE TABLE spatial_reference
(
	id BIGINT NOT NULL,
	version INTEGER NOT NULL DEFAULT 0,
	obj_id BIGINT NOT NULL,
	line INTEGER NOT NULL DEFAULT 0,
	spatial_ref_id BIGINT,
	PRIMARY KEY (id)
) 
;

CREATE INDEX idxSRef_ObjId ON spatial_reference (obj_id ASC);
CREATE INDEX idxSRef_SRefId ON spatial_reference (spatial_ref_id ASC);


CREATE TABLE object_node
(
	id BIGINT NOT NULL,
	version INTEGER NOT NULL DEFAULT 0,
	obj_uuid VARCHAR(40) NOT NULL,
	obj_id BIGINT NOT NULL,
	obj_id_published BIGINT,
	fk_obj_uuid VARCHAR(40),
	PRIMARY KEY (id),
	UNIQUE (obj_uuid)
) 
;

CREATE INDEX idxObjN_ObjId ON object_node (obj_id ASC);
CREATE INDEX idxObjN_ObjIdPub ON object_node (obj_id_published ASC);
CREATE INDEX idxObjN_FObjUuid ON object_node (fk_obj_uuid ASC);


CREATE TABLE object_comment
(
	id BIGINT NOT NULL,
	version INTEGER NOT NULL DEFAULT 0,
	obj_id BIGINT,
	comment TEXT,
	create_uuid VARCHAR(40),
	create_time VARCHAR(17),
	PRIMARY KEY (id)
) 
;

CREATE INDEX idxObjCom_ObjId ON object_comment (obj_id ASC);


CREATE TABLE t01_object
(
	id BIGINT NOT NULL,
	version INTEGER NOT NULL DEFAULT 0,
	obj_uuid VARCHAR(40) NOT NULL,
	obj_name VARCHAR(255),
	org_obj_id VARCHAR(40),
	obj_class INTEGER,
	obj_descr TEXT,
	cat_id BIGINT,
	info_note TEXT,
	avail_access_note TEXT,
	loc_descr TEXT,
	time_from VARCHAR(17),
	time_to VARCHAR(17),
	time_descr TEXT,
	time_period INTEGER,
	time_interval VARCHAR(40),
	time_status INTEGER,
	time_alle VARCHAR(40),
	time_type VARCHAR(5),
	publish_id INTEGER,
	dataset_alternate_name VARCHAR(40),
	dataset_character_set INTEGER,
	dataset_usage TEXT,
	data_language_code CHAR(2),
	metadata_character_set INTEGER,
	metadata_standard_name VARCHAR(80),
	metadata_standard_version VARCHAR(80),
	metadata_language_code CHAR(2),
	vertical_extent_minimum DOUBLE PRECISION,
	vertical_extent_maximum DOUBLE PRECISION,
	vertical_extent_unit INTEGER,
	vertical_extent_vdatum INTEGER,
	fees VARCHAR(255),
	ordering_instructions TEXT,
	is_catalog_data VARCHAR(1) DEFAULT 'N',
	lastexport_time VARCHAR(17),
	expiry_time VARCHAR(17),
	work_state CHAR(1) DEFAULT 'V',
	work_version INTEGER DEFAULT 0,
	mark_deleted CHAR(1) DEFAULT 'N',
	create_time VARCHAR(17),
	mod_time VARCHAR(17),
	mod_uuid VARCHAR(40),
	responsible_uuid VARCHAR(40),
	PRIMARY KEY (id)
) 
;

CREATE INDEX idxObj_CatId ON t01_object (cat_id ASC);
CREATE INDEX idxObjClass ON t01_object (obj_class ASC);


CREATE TABLE t03_catalogue
(
	id BIGINT NOT NULL,
	version INTEGER NOT NULL DEFAULT 0,
	cat_uuid VARCHAR(40) NOT NULL,
	cat_name VARCHAR(255) NOT NULL,
	partner_name VARCHAR(255),
	provider_name VARCHAR(255),
	country_code CHAR(2),
	language_code CHAR(2),
	spatial_ref_id BIGINT,
	workflow_control CHAR(1) DEFAULT 'N',
	expiry_duration INTEGER,
	create_time VARCHAR(17),
	mod_uuid VARCHAR(40),
	mod_time VARCHAR(17),
	PRIMARY KEY (id)
) 
;

CREATE INDEX idxCat_SRefId ON t03_catalogue (spatial_ref_id ASC);


CREATE TABLE t012_obj_adr
(
	id BIGINT NOT NULL,
	version INTEGER NOT NULL DEFAULT 0,
	obj_id BIGINT NOT NULL,
	adr_uuid VARCHAR(40) NOT NULL,
	type INTEGER NOT NULL DEFAULT 0,
	line INTEGER DEFAULT 0,
	special_ref INTEGER DEFAULT 0,
	special_name VARCHAR(80),
	mod_time VARCHAR(17),
	PRIMARY KEY (id),
	UNIQUE (obj_id, adr_uuid, type)
) 
;

CREATE INDEX idxObjAdr_ObjId ON t012_obj_adr (obj_id ASC);
CREATE INDEX idxObjAdr_AdrUuid ON t012_obj_adr (adr_uuid ASC);


CREATE TABLE searchterm_obj
(
	id BIGINT NOT NULL,
	version INTEGER NOT NULL DEFAULT 0,
	obj_id BIGINT NOT NULL,
	line INTEGER NOT NULL DEFAULT 0,
	searchterm_id BIGINT,
	PRIMARY KEY (id)
) 
;


CREATE TABLE permission_addr
(
	id BIGINT NOT NULL,
	version INTEGER NOT NULL DEFAULT 0,
	uuid VARCHAR(40),
	permission_id BIGINT,
	idc_group_id BIGINT,
	PRIMARY KEY (id)
) 
;

CREATE INDEX IDXperm_addr_uuid ON permission_addr (uuid ASC);


CREATE TABLE idc_user
(
	id BIGINT NOT NULL,
	version INTEGER NOT NULL DEFAULT 0,
	parent_id BIGINT,
	addr_uuid VARCHAR(40),
	create_time VARCHAR(17),
	mod_time VARCHAR(17),
	mod_uuid VARCHAR(40),
	idc_group_id BIGINT,
	idc_role INTEGER,
	PRIMARY KEY (id),
	UNIQUE (addr_uuid)
) 
;


CREATE TABLE full_index_addr
(
	id BIGINT NOT NULL,
	version INTEGER NOT NULL DEFAULT 0,
	addr_node_id BIGINT NOT NULL,
	idx_name VARCHAR(255) NOT NULL,
	idx_value TEXT,
	PRIMARY KEY (id)
) 
;

CREATE INDEX idxAddrIdxName ON full_index_addr (idx_name ASC);
CREATE INDEX idxFullIdxAddrId ON full_index_addr (addr_node_id ASC);


CREATE TABLE t08_attr_list
(
	id BIGINT NOT NULL,
	version INTEGER NOT NULL DEFAULT 0,
	attr_type_id BIGINT NOT NULL,
	type CHAR(1),
	listitem_line INTEGER,
	listitem_value VARCHAR(255),
	lang_code CHAR(2),
	PRIMARY KEY (id)
) 
;

CREATE INDEX idxALst_AttrTypeId ON t08_attr_list (attr_type_id ASC);


CREATE TABLE t08_attr
(
	id BIGINT NOT NULL,
	version INTEGER NOT NULL DEFAULT 0,
	attr_type_id BIGINT NOT NULL,
	obj_id BIGINT NOT NULL,
	data VARCHAR(255),
	PRIMARY KEY (id)
) 
;

CREATE INDEX idxAttr_AttrTypeId ON t08_attr (attr_type_id ASC);
CREATE INDEX idxAttr_ObjId ON t08_attr (obj_id ASC);


CREATE TABLE t021_communication
(
	id BIGINT NOT NULL,
	version INTEGER NOT NULL DEFAULT 0,
	adr_id BIGINT NOT NULL,
	line INTEGER NOT NULL DEFAULT 0,
	commtype_key INTEGER,
	commtype_value VARCHAR(20),
	comm_value VARCHAR(80),
	descr VARCHAR(80),
	PRIMARY KEY (id)
) 
;

CREATE INDEX idxComm_AdrId ON t021_communication (adr_id ASC);


CREATE TABLE spatial_ref_value
(
	id BIGINT NOT NULL,
	version INTEGER NOT NULL DEFAULT 0,
	type CHAR(1),
	spatial_ref_sns_id BIGINT,
	name_key INTEGER,
	name_value TEXT,
	nativekey VARCHAR(50),
	x1 DOUBLE PRECISION,
	y1 DOUBLE PRECISION,
	x2 DOUBLE PRECISION,
	y2 DOUBLE PRECISION,
	PRIMARY KEY (id)
) 
;

CREATE INDEX idxSRVal_SRefSNSId ON spatial_ref_value (spatial_ref_sns_id ASC);


CREATE TABLE searchterm_value
(
	id BIGINT NOT NULL,
	version INTEGER NOT NULL DEFAULT 0,
	type CHAR(1),
	searchterm_sns_id BIGINT,
	term TEXT,
	PRIMARY KEY (id)
) 
;

CREATE INDEX idxSTVal_STSNSId ON searchterm_value (searchterm_sns_id ASC);


CREATE TABLE searchterm_adr
(
	id BIGINT NOT NULL,
	version INTEGER NOT NULL DEFAULT 0,
	adr_id BIGINT NOT NULL,
	line INTEGER NOT NULL DEFAULT 0,
	searchterm_id BIGINT,
	PRIMARY KEY (id)
) 
;

CREATE INDEX idxSTAdr_AdrId ON searchterm_adr (adr_id ASC);
CREATE INDEX idxSTAdr_STId ON searchterm_adr (searchterm_id ASC);


CREATE TABLE idc_user_permission
(
	id BIGINT NOT NULL,
	version INTEGER NOT NULL DEFAULT 0,
	permission_id BIGINT,
	idc_group_id BIGINT,
	PRIMARY KEY (id)
) 
;


CREATE TABLE address_node
(
	id BIGINT NOT NULL,
	version INTEGER NOT NULL DEFAULT 0,
	addr_uuid VARCHAR(40) NOT NULL,
	addr_id BIGINT NOT NULL,
	addr_id_published BIGINT,
	fk_addr_uuid VARCHAR(40),
	PRIMARY KEY (id),
	UNIQUE (addr_uuid)
) 
;

CREATE INDEX idxAddrN_AddrId ON address_node (addr_id ASC);
CREATE INDEX idxAddrN_AddrIdPub ON address_node (addr_id_published ASC);
CREATE INDEX idxAddrN_FAddrUuid ON address_node (fk_addr_uuid ASC);


CREATE TABLE address_comment
(
	id BIGINT NOT NULL,
	version INTEGER NOT NULL DEFAULT 0,
	addr_id BIGINT,
	comment TEXT,
	create_uuid VARCHAR(40),
	create_time VARCHAR(17),
	PRIMARY KEY (id)
) 
;

CREATE INDEX idxAddrCom_AddrId ON address_comment (addr_id ASC);


CREATE TABLE t08_attr_type
(
	id BIGINT NOT NULL,
	version INTEGER NOT NULL DEFAULT 0,
	name VARCHAR(50),
	length INTEGER,
	type CHAR(1),
	PRIMARY KEY (id)
) 
;


CREATE TABLE t02_address
(
	id BIGINT NOT NULL,
	version INTEGER NOT NULL DEFAULT 0,
	adr_uuid VARCHAR(40),
	org_adr_id VARCHAR(40),
	adr_type INTEGER,
	institution VARCHAR(255),
	lastname VARCHAR(40),
	firstname VARCHAR(40),
	address_key INTEGER,
	address_value VARCHAR(40),
	title_key INTEGER,
	title_value VARCHAR(40),
	street VARCHAR(80),
	postcode VARCHAR(10),
	postbox VARCHAR(10),
	postbox_pc VARCHAR(10),
	city VARCHAR(80),
	country_code CHAR(2),
	job TEXT,
	descr VARCHAR(255),
	lastexport_time VARCHAR(17),
	expiry_time VARCHAR(17),
	work_state CHAR(1) DEFAULT 'V',
	work_version INTEGER,
	mark_deleted CHAR(1) DEFAULT 'N',
	create_time VARCHAR(17),
	mod_time VARCHAR(17),
	mod_uuid VARCHAR(40),
	responsible_uuid VARCHAR(40),
	PRIMARY KEY (id)
) 
;


CREATE TABLE t011_town_loc_town
(
	township_no VARCHAR(12),
	name VARCHAR(50)
) 
;


CREATE TABLE sys_list
(
	id BIGINT NOT NULL,
	version INTEGER NOT NULL DEFAULT 0,
	lst_id INTEGER NOT NULL DEFAULT 0,
	entry_id INTEGER NOT NULL DEFAULT 0,
	lang_id CHAR(2) NOT NULL,
	name VARCHAR(255),
	description VARCHAR(255),
	maintainable INTEGER DEFAULT 0,
	is_default CHAR(1) DEFAULT 'N',
	PRIMARY KEY (id),
	UNIQUE (entry_id, lst_id, lang_id)
) 
;

CREATE INDEX idxSysList_LstId ON sys_list (lst_id ASC);


CREATE TABLE sys_generic_key
(
	key_name VARCHAR(50) NOT NULL,
	value_string VARCHAR(120),
	PRIMARY KEY (key_name)
) 
;


CREATE TABLE sys_export
(
	export_id BIGINT NOT NULL,
	num_objects INTEGER,
	num_addresses INTEGER,
	exporttime VARCHAR(17),
	PRIMARY KEY (export_id),
	UNIQUE (exporttime)
) 
;


CREATE TABLE spatial_ref_sns
(
	id BIGINT NOT NULL,
	version INTEGER NOT NULL DEFAULT 0,
	sns_id VARCHAR(40),
	expired_at VARCHAR(17),
	PRIMARY KEY (id)
) 
;

CREATE INDEX idxSpatialSnsId ON spatial_ref_sns (sns_id ASC);


CREATE TABLE searchterm_sns
(
	id BIGINT NOT NULL,
	version INTEGER NOT NULL DEFAULT 0,
	sns_id VARCHAR(40),
	expired_at VARCHAR(17),
	PRIMARY KEY (id)
) 
;

CREATE INDEX idxTermSnsId ON searchterm_sns (sns_id ASC);


CREATE TABLE permission
(
	id BIGINT NOT NULL,
	version INTEGER NOT NULL DEFAULT 0,
	class_name VARCHAR(255),
	name VARCHAR(255),
	action VARCHAR(255),
	PRIMARY KEY (id)
) 
;


CREATE TABLE idc_group
(
	id BIGINT NOT NULL,
	version INTEGER NOT NULL DEFAULT 0,
	name VARCHAR(50) NOT NULL,
	create_time VARCHAR(17),
	mod_time VARCHAR(17),
	mod_uuid VARCHAR(40),
	PRIMARY KEY (id),
	UNIQUE (name)
) 
;


CREATE TABLE hibernate_unique_key
(
	next_hi BIGINT
) 
;

