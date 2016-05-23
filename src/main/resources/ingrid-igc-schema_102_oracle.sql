---
-- **************************************************-
-- InGrid UDK-IGC Importer (IGC Updater)
-- ==================================================
-- Copyright (C) 2014 wemove digital solutions GmbH
-- ==================================================
-- Licensed under the EUPL, Version 1.1 or – as soon they will be
-- approved by the European Commission - subsequent versions of the
-- EUPL (the "Licence");
-- 
-- You may not use this work except in compliance with the Licence.
-- You may obtain a copy of the Licence at:
-- 
-- http://ec.europa.eu/idabc/eupl5
-- 
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the Licence is distributed on an "AS IS" basis,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the Licence for the specific language governing permissions and
-- limitations under the Licence.
-- **************************************************#
---
PROMPT Creating Table t011_obj_topic_cat ...
CREATE TABLE t011_obj_topic_cat (
  id NUMBER(24,0) NOT NULL,
  version NUMBER(10,0) DEFAULT '0' NOT NULL,
  obj_id NUMBER(24,0),
  line NUMBER(10,0) DEFAULT '0',
  topic_category NUMBER(10,0)
);


PROMPT Creating Primary Key Constraint PRIMARY_53 on table t011_obj_topic_cat ... 
ALTER TABLE t011_obj_topic_cat
ADD CONSTRAINT PRIMARY_53 PRIMARY KEY
(
  id
)
ENABLE
;

-- DROP TABLE t012_obj_adr CASCADE CONSTRAINTS;


PROMPT Creating Table t012_obj_adr ...
CREATE TABLE t012_obj_adr (
  id NUMBER(24,0) NOT NULL,
  version NUMBER(10,0) DEFAULT '0' NOT NULL,
  obj_id NUMBER(24,0) NOT NULL,
  adr_uuid VARCHAR2(40 CHAR) NOT NULL,
  type NUMBER(10,0) DEFAULT '0' NOT NULL,
  line NUMBER(10,0) DEFAULT '0',
  special_ref NUMBER(10,0) DEFAULT '0',
  special_name VARCHAR2(255 CHAR),
  mod_time VARCHAR2(17 CHAR)
);


PROMPT Creating Primary Key Constraint PRIMARY_54 on table t012_obj_adr ... 
ALTER TABLE t012_obj_adr
ADD CONSTRAINT PRIMARY_54 PRIMARY KEY
(
  id
)
ENABLE
;
PROMPT Creating Index obj_id on t012_obj_adr ...
CREATE INDEX obj_id ON t012_obj_adr
(
  obj_id,
  adr_uuid,
  type
) 
;
PROMPT Creating Index idxObjAdr_ObjId on t012_obj_adr ...
CREATE INDEX idxObjAdr_ObjId ON t012_obj_adr
(
  obj_id
) 
;
PROMPT Creating Index idxObjAdr_AdrUuid on t012_obj_adr ...
CREATE INDEX idxObjAdr_AdrUuid ON t012_obj_adr
(
  adr_uuid
) 
;

-- DROP TABLE t014_info_impart CASCADE CONSTRAINTS;


PROMPT Creating Table t014_info_impart ...
CREATE TABLE t014_info_impart (
  id NUMBER(24,0) NOT NULL,
  version NUMBER(10,0) DEFAULT '0' NOT NULL,
  obj_id NUMBER(24,0),
  line NUMBER(10,0) DEFAULT '0',
  impart_value VARCHAR2(255 CHAR),
  impart_key NUMBER(10,0)
);


PROMPT Creating Primary Key Constraint PRIMARY_55 on table t014_info_impart ... 
ALTER TABLE t014_info_impart
ADD CONSTRAINT PRIMARY_55 PRIMARY KEY
(
  id
)
ENABLE
;
PROMPT Creating Index idxInfImpart_ObjId on t014_info_impart ...
CREATE INDEX idxInfImpart_ObjId ON t014_info_impart
(
  obj_id
) 
;

-- DROP TABLE t015_legist CASCADE CONSTRAINTS;


PROMPT Creating Table t015_legist ...
CREATE TABLE t015_legist (
  id NUMBER(24,0) NOT NULL,
  version NUMBER(10,0) DEFAULT '0' NOT NULL,
  obj_id NUMBER(24,0),
  line NUMBER(10,0) DEFAULT '0',
  legist_value VARCHAR2(255 CHAR),
  legist_key NUMBER(10,0)
);


PROMPT Creating Primary Key Constraint PRIMARY_56 on table t015_legist ... 
ALTER TABLE t015_legist
ADD CONSTRAINT PRIMARY_56 PRIMARY KEY
(
  id
)
ENABLE
;
PROMPT Creating Index idxLegis_ObjId on t015_legist ...
CREATE INDEX idxLegis_ObjId ON t015_legist
(
  obj_id
) 
;

-- DROP TABLE t017_url_ref CASCADE CONSTRAINTS;


PROMPT Creating Table t017_url_ref ...
CREATE TABLE t017_url_ref (
  id NUMBER(24,0) NOT NULL,
  version NUMBER(10,0) DEFAULT '0' NOT NULL,
  obj_id NUMBER(24,0),
  line NUMBER(10,0) DEFAULT '0',
  url_link VARCHAR2(255 CHAR),
  special_ref NUMBER(10,0),
  special_name VARCHAR2(80 CHAR),
  content VARCHAR2(255 CHAR),
  datatype_key NUMBER(10,0),
  datatype_value VARCHAR2(40 CHAR),
  volume VARCHAR2(20 CHAR),
  icon VARCHAR2(255 CHAR),
  icon_text VARCHAR2(80 CHAR),
  descr CLOB,
  url_type NUMBER(10,0)
);


PROMPT Creating Primary Key Constraint PRIMARY_57 on table t017_url_ref ... 
ALTER TABLE t017_url_ref
ADD CONSTRAINT PRIMARY_57 PRIMARY KEY
(
  id
)
ENABLE
;
PROMPT Creating Index idxUrlRef_ObjId on t017_url_ref ...
CREATE INDEX idxUrlRef_ObjId ON t017_url_ref
(
  obj_id
) 
;

-- DROP TABLE t01_object CASCADE CONSTRAINTS;


PROMPT Creating Table t01_object ...
CREATE TABLE t01_object (
  id NUMBER(24,0) NOT NULL,
  version NUMBER(10,0) DEFAULT '0' NOT NULL,
  obj_uuid VARCHAR2(40 CHAR) NOT NULL,
  obj_name VARCHAR2(255 CHAR),
  org_obj_id VARCHAR2(255 CHAR),
  obj_class NUMBER(10,0),
  obj_descr CLOB,
  cat_id NUMBER(24,0),
  info_note CLOB,
  avail_access_note CLOB,
  loc_descr CLOB,
  time_from VARCHAR2(17 CHAR),
  time_to VARCHAR2(17 CHAR),
  time_descr CLOB,
  time_period NUMBER(10,0),
  time_interval VARCHAR2(40 CHAR),
  time_status NUMBER(10,0),
  time_alle VARCHAR2(40 CHAR),
  time_type VARCHAR2(5 CHAR),
  publish_id NUMBER(10,0),
  dataset_alternate_name VARCHAR2(255 CHAR),
  dataset_character_set NUMBER(10,0),
  dataset_usage CLOB,
  data_language_code CHAR(2 CHAR),
  metadata_character_set NUMBER(10,0),
  metadata_standard_name VARCHAR2(255 CHAR),
  metadata_standard_version VARCHAR2(255 CHAR),
  metadata_language_code CHAR(2 CHAR),
  vertical_extent_minimum FLOAT,
  vertical_extent_maximum FLOAT,
  vertical_extent_unit NUMBER(10,0),
  vertical_extent_vdatum NUMBER(10,0),
  fees VARCHAR2(255 CHAR),
  ordering_instructions CLOB,
  is_catalog_data VARCHAR2(1 CHAR) DEFAULT 'N',
  lastexport_time VARCHAR2(17 CHAR),
  expiry_time VARCHAR2(17 CHAR),
  work_state CHAR(1 CHAR) DEFAULT 'V',
  work_version NUMBER(10,0) DEFAULT '0',
  mark_deleted CHAR(1 CHAR) DEFAULT 'N',
  create_time VARCHAR2(17 CHAR),
  mod_time VARCHAR2(17 CHAR),
  mod_uuid VARCHAR2(40 CHAR),
  responsible_uuid VARCHAR2(40 CHAR)
);


PROMPT Creating Primary Key Constraint PRIMARY_58 on table t01_object ... 
ALTER TABLE t01_object
ADD CONSTRAINT PRIMARY_58 PRIMARY KEY
(
  id
)
ENABLE
;
PROMPT Creating Index idxObj_CatId on t01_object ...
CREATE INDEX idxObj_CatId ON t01_object
(
  cat_id
) 
;
PROMPT Creating Index idxObjClass on t01_object ...
CREATE INDEX idxObjClass ON t01_object
(
  obj_class
) 
;

-- DROP TABLE t021_communication CASCADE CONSTRAINTS;


PROMPT Creating Table t021_communication ...
CREATE TABLE t021_communication (
  id NUMBER(24,0) NOT NULL,
  version NUMBER(10,0) DEFAULT '0' NOT NULL,
  adr_id NUMBER(24,0) NOT NULL,
  line NUMBER(10,0) DEFAULT '0' NOT NULL,
  commtype_key NUMBER(10,0),
  commtype_value VARCHAR2(255 CHAR),
  comm_value VARCHAR2(255 CHAR),
  descr VARCHAR2(255 CHAR)
);


PROMPT Creating Primary Key Constraint PRIMARY_59 on table t021_communication ... 
ALTER TABLE t021_communication
ADD CONSTRAINT PRIMARY_59 PRIMARY KEY
(
  id
)
ENABLE
;
PROMPT Creating Index idxComm_AdrId on t021_communication ...
CREATE INDEX idxComm_AdrId ON t021_communication
(
  adr_id
) 
;

-- DROP TABLE t02_address CASCADE CONSTRAINTS;


PROMPT Creating Table t02_address ...
CREATE TABLE t02_address (
  id NUMBER(24,0) NOT NULL,
  version NUMBER(10,0) DEFAULT '0' NOT NULL,
  adr_uuid VARCHAR2(40 CHAR),
  org_adr_id VARCHAR2(255 CHAR),
  adr_type NUMBER(10,0),
  institution VARCHAR2(255 CHAR),
  lastname VARCHAR2(255 CHAR),
  firstname VARCHAR2(255 CHAR),
  address_key NUMBER(10,0),
  address_value VARCHAR2(255 CHAR),
  title_key NUMBER(10,0),
  title_value VARCHAR2(255 CHAR),
  street VARCHAR2(255 CHAR),
  postcode VARCHAR2(255 CHAR),
  postbox VARCHAR2(255 CHAR),
  postbox_pc VARCHAR2(255 CHAR),
  city VARCHAR2(255 CHAR),
  country_code CHAR(2 CHAR),
  job CLOB,
  descr VARCHAR2(255 CHAR),
  lastexport_time VARCHAR2(17 CHAR),
  expiry_time VARCHAR2(17 CHAR),
  work_state CHAR(1 CHAR) DEFAULT 'V',
  work_version NUMBER(10,0),
  mark_deleted CHAR(1 CHAR) DEFAULT 'N',
  create_time VARCHAR2(17 CHAR),
  mod_time VARCHAR2(17 CHAR),
  mod_uuid VARCHAR2(40 CHAR),
  responsible_uuid VARCHAR2(40 CHAR)
);


PROMPT Creating Primary Key Constraint PRIMARY_60 on table t02_address ... 
ALTER TABLE t02_address
ADD CONSTRAINT PRIMARY_60 PRIMARY KEY
(
  id
)
ENABLE
;

-- DROP TABLE t03_catalogue CASCADE CONSTRAINTS;


PROMPT Creating Table t03_catalogue ...
CREATE TABLE t03_catalogue (
  id NUMBER(24,0) NOT NULL,
  version NUMBER(10,0) DEFAULT '0' NOT NULL,
  cat_uuid VARCHAR2(40 CHAR) NOT NULL,
  cat_name VARCHAR2(255 CHAR) NOT NULL,
  partner_name VARCHAR2(255 CHAR),
  provider_name VARCHAR2(255 CHAR),
  country_code CHAR(2 CHAR),
  language_code CHAR(2 CHAR),
  spatial_ref_id NUMBER(24,0),
  workflow_control CHAR(1 CHAR) DEFAULT 'N',
  expiry_duration NUMBER(10,0),
  create_time VARCHAR2(17 CHAR),
  mod_uuid VARCHAR2(40 CHAR),
  mod_time VARCHAR2(17 CHAR)
);


PROMPT Creating Primary Key Constraint PRIMARY_61 on table t03_catalogue ... 
ALTER TABLE t03_catalogue
ADD CONSTRAINT PRIMARY_61 PRIMARY KEY
(
  id
)
ENABLE
;
PROMPT Creating Index idxCat_SRefId on t03_catalogue ...
CREATE INDEX idxCat_SRefId ON t03_catalogue
(
  spatial_ref_id
) 
;

-- DROP TABLE t08_attr_list CASCADE CONSTRAINTS;


PROMPT Creating Table t08_attr_list ...
CREATE TABLE t08_attr_list (
  id NUMBER(24,0) NOT NULL,
  version NUMBER(10,0) DEFAULT '0' NOT NULL,
  attr_type_id NUMBER(24,0) NOT NULL,
  type CHAR(1 CHAR),
  listitem_line NUMBER(10,0),
  listitem_value VARCHAR2(255 CHAR),
  lang_code CHAR(2 CHAR)
);


PROMPT Creating Primary Key Constraint PRIMARY_63 on table t08_attr_list ... 
ALTER TABLE t08_attr_list
ADD CONSTRAINT PRIMARY_63 PRIMARY KEY
(
  id
)
ENABLE
;
PROMPT Creating Index idxALst_AttrTypeId on t08_attr_list ...
CREATE INDEX idxALst_AttrTypeId ON t08_attr_list
(
  attr_type_id
) 
;

-- DROP TABLE address_comment CASCADE CONSTRAINTS;


PROMPT Creating Table address_comment ...
CREATE TABLE address_comment (
  id NUMBER(24,0) NOT NULL,
  version NUMBER(10,0) DEFAULT '0' NOT NULL,
  addr_id NUMBER(24,0),
  comment_ CLOB,
  create_uuid VARCHAR2(40 CHAR),
  create_time VARCHAR2(17 CHAR)
);


PROMPT Creating Primary Key Constraint PRIMARY on table address_comment ... 
ALTER TABLE address_comment
ADD CONSTRAINT PRIMARY PRIMARY KEY
(
  id
)
ENABLE
;
PROMPT Creating Index idxAddrCom_AddrId on address_comment ...
CREATE INDEX idxAddrCom_AddrId ON address_comment
(
  addr_id
) 
;

-- DROP TABLE address_node CASCADE CONSTRAINTS;


PROMPT Creating Table address_node ...
CREATE TABLE address_node (
  id NUMBER(24,0) NOT NULL,
  version NUMBER(10,0) DEFAULT '0' NOT NULL,
  addr_uuid VARCHAR2(40 CHAR) NOT NULL,
  addr_id NUMBER(24,0) NOT NULL,
  addr_id_published NUMBER(24,0),
  fk_addr_uuid VARCHAR2(40 CHAR)
);


PROMPT Creating Primary Key Constraint PRIMARY_64 on table address_node ... 
ALTER TABLE address_node
ADD CONSTRAINT PRIMARY_64 PRIMARY KEY
(
  id
)
ENABLE
;
PROMPT Creating Unique Index addr_uuid on address_node...
CREATE UNIQUE INDEX addr_uuid ON address_node
(
  addr_uuid
) 
;
PROMPT Creating Index idxAddrN_AddrId on address_node ...
CREATE INDEX idxAddrN_AddrId ON address_node
(
  addr_id
) 
;
PROMPT Creating Index idxAddrN_AddrIdPub on address_node ...
CREATE INDEX idxAddrN_AddrIdPub ON address_node
(
  addr_id_published
) 
;
PROMPT Creating Index idxAddrN_FAddrUuid on address_node ...
CREATE INDEX idxAddrN_FAddrUuid ON address_node
(
  fk_addr_uuid
) 
;

-- DROP TABLE full_index_addr CASCADE CONSTRAINTS;


PROMPT Creating Table full_index_addr ...
CREATE TABLE full_index_addr (
  id NUMBER(24,0) NOT NULL,
  version NUMBER(10,0) DEFAULT '0' NOT NULL,
  addr_node_id NUMBER(24,0) NOT NULL,
  idx_name VARCHAR2(255 CHAR) NOT NULL,
  idx_value CLOB
);


PROMPT Creating Primary Key Constraint PRIMARY_3 on table full_index_addr ... 
ALTER TABLE full_index_addr
ADD CONSTRAINT PRIMARY_3 PRIMARY KEY
(
  id
)
ENABLE
;
PROMPT Creating Index idxAddrIdxName on full_index_addr ...
CREATE INDEX idxAddrIdxName ON full_index_addr
(
  idx_name
) 
;
PROMPT Creating Index idxFullIdxAddrId on full_index_addr ...
CREATE INDEX idxFullIdxAddrId ON full_index_addr
(
  addr_node_id
) 
;

-- DROP TABLE full_index_obj CASCADE CONSTRAINTS;


PROMPT Creating Table full_index_obj ...
CREATE TABLE full_index_obj (
  id NUMBER(24,0) NOT NULL,
  version NUMBER(10,0) DEFAULT '0' NOT NULL,
  obj_node_id NUMBER(24,0) NOT NULL,
  idx_name VARCHAR2(255 CHAR) NOT NULL,
  idx_value CLOB
);


PROMPT Creating Primary Key Constraint PRIMARY_4 on table full_index_obj ... 
ALTER TABLE full_index_obj
ADD CONSTRAINT PRIMARY_4 PRIMARY KEY
(
  id
)
ENABLE
;
PROMPT Creating Index idxObjIdxName on full_index_obj ...
CREATE INDEX idxObjIdxName ON full_index_obj
(
  idx_name
) 
;
PROMPT Creating Index idxFullObjNodedId on full_index_obj ...
CREATE INDEX idxFullObjNodedId ON full_index_obj
(
  obj_node_id
) 
;

-- DROP TABLE hibernate_unique_key CASCADE CONSTRAINTS;


PROMPT Creating Table hibernate_unique_key ...
CREATE TABLE hibernate_unique_key (
  next_hi NUMBER(24,0)
);



-- DROP TABLE t011_obj_serv_operation CASCADE CONSTRAINTS;


PROMPT Creating Table t011_obj_serv_operation ...
CREATE TABLE t011_obj_serv_operation (
  id NUMBER(24,0) NOT NULL,
  version NUMBER(10,0) DEFAULT '0' NOT NULL,
  obj_serv_id NUMBER(24,0),
  line NUMBER(10,0) DEFAULT '0',
  name_key NUMBER(10,0),
  name_value VARCHAR2(255 CHAR),
  descr CLOB,
  invocation_name VARCHAR2(255 CHAR)
);


PROMPT Creating Primary Key Constraint PRIMARY_49 on table t011_obj_serv_operation ... 
ALTER TABLE t011_obj_serv_operation
ADD CONSTRAINT PRIMARY_49 PRIMARY KEY
(
  id
)
ENABLE
;
PROMPT Creating Index idxOSerOper_OSerId on t011_obj_serv_operation ...
CREATE INDEX idxOSerOper_OSerId ON t011_obj_serv_operation
(
  obj_serv_id
) 
;

-- DROP TABLE t011_obj_serv_version CASCADE CONSTRAINTS;


PROMPT Creating Table t011_obj_serv_version ...
CREATE TABLE t011_obj_serv_version (
  id NUMBER(24,0) NOT NULL,
  version NUMBER(10,0) DEFAULT '0' NOT NULL,
  obj_serv_id NUMBER(24,0),
  line NUMBER(10,0) DEFAULT '0',
  serv_version VARCHAR2(255 CHAR)
);


PROMPT Creating Primary Key Constraint PRIMARY_52 on table t011_obj_serv_version ... 
ALTER TABLE t011_obj_serv_version
ADD CONSTRAINT PRIMARY_52 PRIMARY KEY
(
  id
)
ENABLE
;
PROMPT Creating Index idxOSerVers_OSerId on t011_obj_serv_version ...
CREATE INDEX idxOSerVers_OSerId ON t011_obj_serv_version
(
  obj_serv_id
) 
;

-- DROP TABLE t011_obj_data_para CASCADE CONSTRAINTS;


PROMPT Creating Table t011_obj_data_para ...
CREATE TABLE t011_obj_data_para (
  id NUMBER(24,0) NOT NULL,
  version NUMBER(10,0) DEFAULT '0' NOT NULL,
  obj_id NUMBER(24,0),
  line NUMBER(10,0),
  parameter VARCHAR2(255 CHAR),
  unit VARCHAR2(255 CHAR)
);


PROMPT Creating Primary Key Constraint PRIMARY_34 on table t011_obj_data_para ... 
ALTER TABLE t011_obj_data_para
ADD CONSTRAINT PRIMARY_34 PRIMARY KEY
(
  id
)
ENABLE
;
PROMPT Creating Index idxODataPara_ObjId on t011_obj_data_para ...
CREATE INDEX idxODataPara_ObjId ON t011_obj_data_para
(
  obj_id
) 
;

-- DROP TABLE t011_obj_geo CASCADE CONSTRAINTS;


PROMPT Creating Table t011_obj_geo ...
CREATE TABLE t011_obj_geo (
  id NUMBER(24,0) NOT NULL,
  version NUMBER(10,0) DEFAULT '0' NOT NULL,
  obj_id NUMBER(24,0),
  special_base CLOB,
  data_base CLOB,
  method CLOB,
  referencesystem_value VARCHAR2(255 CHAR),
  referencesystem_key NUMBER(10,0),
  rec_exact FLOAT,
  rec_grade FLOAT,
  hierarchy_level NUMBER(10,0),
  vector_topology_level NUMBER(10,0),
  pos_accuracy_vertical FLOAT,
  keyc_incl_w_dataset NUMBER(10,0) DEFAULT '0'
);


PROMPT Creating Primary Key Constraint PRIMARY_35 on table t011_obj_geo ... 
ALTER TABLE t011_obj_geo
ADD CONSTRAINT PRIMARY_35 PRIMARY KEY
(
  id
)
ENABLE
;
PROMPT Creating Index idxOGeo_ObjId on t011_obj_geo ...
CREATE INDEX idxOGeo_ObjId ON t011_obj_geo
(
  obj_id
) 
;

-- DROP TABLE t011_obj_geo_keyc CASCADE CONSTRAINTS;


PROMPT Creating Table t011_obj_geo_keyc ...
CREATE TABLE t011_obj_geo_keyc (
  id NUMBER(24,0) NOT NULL,
  version NUMBER(10,0) DEFAULT '0' NOT NULL,
  obj_geo_id NUMBER(24,0),
  line NUMBER(10,0) DEFAULT '0',
  keyc_key NUMBER(10,0),
  keyc_value VARCHAR2(255 CHAR),
  key_date VARCHAR2(17 CHAR),
  edition VARCHAR2(255 CHAR)
);


PROMPT Creating Primary Key Constraint PRIMARY_36 on table t011_obj_geo_keyc ... 
ALTER TABLE t011_obj_geo_keyc
ADD CONSTRAINT PRIMARY_36 PRIMARY KEY
(
  id
)
ENABLE
;
PROMPT Creating Index idxOGeoKeyc_OGeoId on t011_obj_geo_keyc ...
CREATE INDEX idxOGeoKeyc_OGeoId ON t011_obj_geo_keyc
(
  obj_geo_id
) 
;

-- DROP TABLE t011_obj_geo_scale CASCADE CONSTRAINTS;


PROMPT Creating Table t011_obj_geo_scale ...
CREATE TABLE t011_obj_geo_scale (
  id NUMBER(24,0) NOT NULL,
  version NUMBER(10,0) DEFAULT '0' NOT NULL,
  obj_geo_id NUMBER(24,0),
  line NUMBER(10,0) DEFAULT '0',
  scale NUMBER(10,0),
  resolution_ground FLOAT,
  resolution_scan FLOAT
);


PROMPT Creating Primary Key Constraint PRIMARY_37 on table t011_obj_geo_scale ... 
ALTER TABLE t011_obj_geo_scale
ADD CONSTRAINT PRIMARY_37 PRIMARY KEY
(
  id
)
ENABLE
;
PROMPT Creating Index idxOGeoScal_OGeoId on t011_obj_geo_scale ...
CREATE INDEX idxOGeoScal_OGeoId ON t011_obj_geo_scale
(
  obj_geo_id
) 
;

-- DROP TABLE t011_obj_geo_supplinfo CASCADE CONSTRAINTS;


PROMPT Creating Table t011_obj_geo_supplinfo ...
CREATE TABLE t011_obj_geo_supplinfo (
  id NUMBER(24,0) NOT NULL,
  version NUMBER(10,0) DEFAULT '0' NOT NULL,
  obj_geo_id NUMBER(24,0),
  line NUMBER(10,0) DEFAULT '0',
  feature_type VARCHAR2(255 CHAR)
);


PROMPT Creating Primary Key Constraint PRIMARY_39 on table t011_obj_geo_supplinfo ... 
ALTER TABLE t011_obj_geo_supplinfo
ADD CONSTRAINT PRIMARY_39 PRIMARY KEY
(
  id
)
ENABLE
;
PROMPT Creating Index idxOGeoSupp_OGeoId on t011_obj_geo_supplinfo ...
CREATE INDEX idxOGeoSupp_OGeoId ON t011_obj_geo_supplinfo
(
  obj_geo_id
) 
;

-- DROP TABLE t011_obj_geo_symc CASCADE CONSTRAINTS;


PROMPT Creating Table t011_obj_geo_symc ...
CREATE TABLE t011_obj_geo_symc (
  id NUMBER(24,0) NOT NULL,
  version NUMBER(10,0) DEFAULT '0' NOT NULL,
  obj_geo_id NUMBER(24,0),
  line NUMBER(10,0) DEFAULT '0',
  symbol_cat_key NUMBER(10,0),
  symbol_cat_value VARCHAR2(255 CHAR),
  symbol_date VARCHAR2(17 CHAR),
  edition VARCHAR2(255 CHAR)
);


PROMPT Creating Primary Key Constraint PRIMARY_40 on table t011_obj_geo_symc ... 
ALTER TABLE t011_obj_geo_symc
ADD CONSTRAINT PRIMARY_40 PRIMARY KEY
(
  id
)
ENABLE
;
PROMPT Creating Index idxOGeoSymc_OGeoId on t011_obj_geo_symc ...
CREATE INDEX idxOGeoSymc_OGeoId ON t011_obj_geo_symc
(
  obj_geo_id
) 
;

-- DROP TABLE t011_obj_geo_vector CASCADE CONSTRAINTS;


PROMPT Creating Table t011_obj_geo_vector ...
CREATE TABLE t011_obj_geo_vector (
  id NUMBER(24,0) NOT NULL,
  version NUMBER(10,0) DEFAULT '0' NOT NULL,
  obj_geo_id NUMBER(24,0),
  line NUMBER(10,0) DEFAULT '0',
  geometric_object_type NUMBER(10,0),
  geometric_object_count NUMBER(10,0)
);


PROMPT Creating Primary Key Constraint PRIMARY_41 on table t011_obj_geo_vector ... 
ALTER TABLE t011_obj_geo_vector
ADD CONSTRAINT PRIMARY_41 PRIMARY KEY
(
  id
)
ENABLE
;
PROMPT Creating Index idxOGeoVect_OGeoId on t011_obj_geo_vector ...
CREATE INDEX idxOGeoVect_OGeoId ON t011_obj_geo_vector
(
  obj_geo_id
) 
;

-- DROP TABLE t011_obj_literature CASCADE CONSTRAINTS;


PROMPT Creating Table t011_obj_literature ...
CREATE TABLE t011_obj_literature (
  id NUMBER(24,0) NOT NULL,
  version NUMBER(10,0) DEFAULT '0' NOT NULL,
  obj_id NUMBER(24,0),
  author VARCHAR2(255 CHAR),
  publisher VARCHAR2(255 CHAR),
  type_key NUMBER(10,0),
  type_value VARCHAR2(255 CHAR),
  publish_in VARCHAR2(255 CHAR),
  volume VARCHAR2(255 CHAR),
  sides VARCHAR2(255 CHAR),
  publish_year VARCHAR2(255 CHAR),
  publish_loc VARCHAR2(255 CHAR),
  loc VARCHAR2(255 CHAR),
  doc_info VARCHAR2(255 CHAR),
  base CLOB,
  isbn VARCHAR2(255 CHAR),
  publishing VARCHAR2(255 CHAR),
  description CLOB
);


PROMPT Creating Primary Key Constraint PRIMARY_42 on table t011_obj_literature ... 
ALTER TABLE t011_obj_literature
ADD CONSTRAINT PRIMARY_42 PRIMARY KEY
(
  id
)
ENABLE
;
PROMPT Creating Index idxOLit_ObjId on t011_obj_literature ...
CREATE INDEX idxOLit_ObjId ON t011_obj_literature
(
  obj_id
) 
;

-- DROP TABLE t011_obj_serv CASCADE CONSTRAINTS;


PROMPT Creating Table t011_obj_serv ...
CREATE TABLE t011_obj_serv (
  id NUMBER(24,0) NOT NULL,
  version NUMBER(10,0) DEFAULT '0' NOT NULL,
  obj_id NUMBER(24,0),
  type_key NUMBER(10,0),
  type_value VARCHAR2(255 CHAR),
  history CLOB,
  environment CLOB,
  base VARCHAR2(255 CHAR),
  description CLOB
);


PROMPT Creating Primary Key Constraint PRIMARY_44 on table t011_obj_serv ... 
ALTER TABLE t011_obj_serv
ADD CONSTRAINT PRIMARY_44 PRIMARY KEY
(
  id
)
ENABLE
;
PROMPT Creating Index idxObjServ_ObjId on t011_obj_serv ...
CREATE INDEX idxObjServ_ObjId ON t011_obj_serv
(
  obj_id
) 
;

-- DROP TABLE t011_obj_serv_op_connpoint CASCADE CONSTRAINTS;


PROMPT Creating Table t011_obj_serv_op_connpoint ...
CREATE TABLE t011_obj_serv_op_connpoint (
  id NUMBER(24,0) NOT NULL,
  version NUMBER(10,0) DEFAULT '0' NOT NULL,
  obj_serv_op_id NUMBER(24,0),
  line NUMBER(10,0) DEFAULT '0',
  connect_point VARCHAR2(255 CHAR)
);


PROMPT Creating Primary Key Constraint PRIMARY_45 on table t011_obj_serv_op_connpoint ... 
ALTER TABLE t011_obj_serv_op_connpoint
ADD CONSTRAINT PRIMARY_45 PRIMARY KEY
(
  id
)
ENABLE
;
PROMPT Creating Index idxOSerOCP_OSerOId on t011_obj_serv_op_connpoint ...
CREATE INDEX idxOSerOCP_OSerOId ON t011_obj_serv_op_connpoint
(
  obj_serv_op_id
) 
;

-- DROP TABLE idc_group CASCADE CONSTRAINTS;


PROMPT Creating Table idc_group ...
CREATE TABLE idc_group (
  id NUMBER(24,0) NOT NULL,
  version NUMBER(10,0) DEFAULT '0' NOT NULL,
  name VARCHAR2(255 CHAR),
  create_time VARCHAR2(17 CHAR),
  mod_time VARCHAR2(17 CHAR),
  mod_uuid VARCHAR2(40 CHAR)
);


PROMPT Creating Primary Key Constraint PRIMARY_5 on table idc_group ... 
ALTER TABLE idc_group
ADD CONSTRAINT PRIMARY_5 PRIMARY KEY
(
  id
)
ENABLE
;
PROMPT Creating Unique Index name on idc_group...
CREATE UNIQUE INDEX name ON idc_group
(
  name
) 
;

-- DROP TABLE idc_user CASCADE CONSTRAINTS;


PROMPT Creating Table idc_user ...
CREATE TABLE idc_user (
  id NUMBER(24,0) NOT NULL,
  version NUMBER(10,0) DEFAULT '0' NOT NULL,
  parent_id NUMBER(24,0),
  addr_uuid VARCHAR2(40 CHAR),
  create_time VARCHAR2(17 CHAR),
  mod_time VARCHAR2(17 CHAR),
  mod_uuid VARCHAR2(40 CHAR),
  idc_group_id NUMBER(24,0),
  idc_role NUMBER(10,0)
);


PROMPT Creating Primary Key Constraint PRIMARY_6 on table idc_user ... 
ALTER TABLE idc_user
ADD CONSTRAINT PRIMARY_6 PRIMARY KEY
(
  id
)
ENABLE
;
PROMPT Creating Unique Index addr_uuid_1 on idc_user...
CREATE UNIQUE INDEX addr_uuid_1 ON idc_user
(
  addr_uuid
) 
;

-- DROP TABLE idc_user_permission CASCADE CONSTRAINTS;


PROMPT Creating Table idc_user_permission ...
CREATE TABLE idc_user_permission (
  id NUMBER(24,0) NOT NULL,
  version NUMBER(10,0) DEFAULT '0' NOT NULL,
  permission_id NUMBER(24,0),
  idc_group_id NUMBER(24,0)
);


PROMPT Creating Primary Key Constraint PRIMARY_7 on table idc_user_permission ... 
ALTER TABLE idc_user_permission
ADD CONSTRAINT PRIMARY_7 PRIMARY KEY
(
  id
)
ENABLE
;

-- DROP TABLE object_comment CASCADE CONSTRAINTS;


PROMPT Creating Table object_comment ...
CREATE TABLE object_comment (
  id NUMBER(24,0) NOT NULL,
  version NUMBER(10,0) DEFAULT '0' NOT NULL,
  obj_id NUMBER(24,0),
  comment_ CLOB,
  create_uuid VARCHAR2(40 CHAR),
  create_time VARCHAR2(17 CHAR)
);


PROMPT Creating Primary Key Constraint PRIMARY_9 on table object_comment ... 
ALTER TABLE object_comment
ADD CONSTRAINT PRIMARY_9 PRIMARY KEY
(
  id
)
ENABLE
;
PROMPT Creating Index idxObjCom_ObjId on object_comment ...
CREATE INDEX idxObjCom_ObjId ON object_comment
(
  obj_id
) 
;

-- DROP TABLE object_node CASCADE CONSTRAINTS;


PROMPT Creating Table object_node ...
CREATE TABLE object_node (
  id NUMBER(24,0) NOT NULL,
  version NUMBER(10,0) DEFAULT '0' NOT NULL,
  obj_uuid VARCHAR2(40 CHAR) NOT NULL,
  obj_id NUMBER(24,0) NOT NULL,
  obj_id_published NUMBER(24,0),
  fk_obj_uuid VARCHAR2(40 CHAR)
);


PROMPT Creating Primary Key Constraint PRIMARY_12 on table object_node ... 
ALTER TABLE object_node
ADD CONSTRAINT PRIMARY_12 PRIMARY KEY
(
  id
)
ENABLE
;
PROMPT Creating Unique Index obj_uuid on object_node...
CREATE UNIQUE INDEX obj_uuid ON object_node
(
  obj_uuid
) 
;
PROMPT Creating Index idxObjN_ObjId on object_node ...
CREATE INDEX idxObjN_ObjId ON object_node
(
  obj_id
) 
;
PROMPT Creating Index idxObjN_ObjIdPub on object_node ...
CREATE INDEX idxObjN_ObjIdPub ON object_node
(
  obj_id_published
) 
;
PROMPT Creating Index idxObjN_FObjUuid on object_node ...
CREATE INDEX idxObjN_FObjUuid ON object_node
(
  fk_obj_uuid
) 
;

-- DROP TABLE object_reference CASCADE CONSTRAINTS;


PROMPT Creating Table object_reference ...
CREATE TABLE object_reference (
  id NUMBER(24,0) NOT NULL,
  version NUMBER(10,0) DEFAULT '0' NOT NULL,
  obj_from_id NUMBER(24,0) NOT NULL,
  obj_to_uuid VARCHAR2(40 CHAR) NOT NULL,
  line NUMBER(10,0) DEFAULT '0',
  special_ref NUMBER(10,0) DEFAULT '0',
  special_name VARCHAR2(255 CHAR),
  descr CLOB
);


PROMPT Creating Primary Key Constraint PRIMARY_13 on table object_reference ... 
ALTER TABLE object_reference
ADD CONSTRAINT PRIMARY_13 PRIMARY KEY
(
  id
)
ENABLE
;
PROMPT Creating Index idxObjRef_OFromId on object_reference ...
CREATE INDEX idxObjRef_OFromId ON object_reference
(
  obj_from_id
) 
;

-- DROP TABLE permission CASCADE CONSTRAINTS;


PROMPT Creating Table permission ...
CREATE TABLE permission (
  id NUMBER(24,0) NOT NULL,
  version NUMBER(10,0) DEFAULT '0' NOT NULL,
  class_name VARCHAR2(255 CHAR),
  name VARCHAR2(255 CHAR),
  action VARCHAR2(255 CHAR)
);


PROMPT Creating Primary Key Constraint PRIMARY_14 on table permission ... 
ALTER TABLE permission
ADD CONSTRAINT PRIMARY_14 PRIMARY KEY
(
  id
)
ENABLE
;

-- DROP TABLE permission_addr CASCADE CONSTRAINTS;


PROMPT Creating Table permission_addr ...
CREATE TABLE permission_addr (
  id NUMBER(24,0) NOT NULL,
  version NUMBER(10,0) DEFAULT '0' NOT NULL,
  uuid VARCHAR2(40 CHAR),
  permission_id NUMBER(24,0),
  idc_group_id NUMBER(24,0)
);


PROMPT Creating Primary Key Constraint PRIMARY_15 on table permission_addr ... 
ALTER TABLE permission_addr
ADD CONSTRAINT PRIMARY_15 PRIMARY KEY
(
  id
)
ENABLE
;
PROMPT Creating Index IDXperm_addr_uuid on permission_addr ...
CREATE INDEX IDXperm_addr_uuid ON permission_addr
(
  uuid
) 
;

-- DROP TABLE permission_obj CASCADE CONSTRAINTS;


PROMPT Creating Table permission_obj ...
CREATE TABLE permission_obj (
  id NUMBER(24,0) NOT NULL,
  version NUMBER(10,0) DEFAULT '0' NOT NULL,
  uuid VARCHAR2(40 CHAR),
  permission_id NUMBER(24,0),
  idc_group_id NUMBER(24,0)
);


PROMPT Creating Primary Key Constraint PRIMARY_16 on table permission_obj ... 
ALTER TABLE permission_obj
ADD CONSTRAINT PRIMARY_16 PRIMARY KEY
(
  id
)
ENABLE
;
PROMPT Creating Index IDXperm_obj_uuid on permission_obj ...
CREATE INDEX IDXperm_obj_uuid ON permission_obj
(
  uuid
) 
;

-- DROP TABLE searchterm_adr CASCADE CONSTRAINTS;


PROMPT Creating Table searchterm_adr ...
CREATE TABLE searchterm_adr (
  id NUMBER(24,0) NOT NULL,
  version NUMBER(10,0) DEFAULT '0' NOT NULL,
  adr_id NUMBER(24,0) NOT NULL,
  line NUMBER(10,0) DEFAULT '0' NOT NULL,
  searchterm_id NUMBER(24,0)
);


PROMPT Creating Primary Key Constraint PRIMARY_17 on table searchterm_adr ... 
ALTER TABLE searchterm_adr
ADD CONSTRAINT PRIMARY_17 PRIMARY KEY
(
  id
)
ENABLE
;
PROMPT Creating Index idxSTAdr_AdrId on searchterm_adr ...
CREATE INDEX idxSTAdr_AdrId ON searchterm_adr
(
  adr_id
) 
;
PROMPT Creating Index idxSTAdr_STId on searchterm_adr ...
CREATE INDEX idxSTAdr_STId ON searchterm_adr
(
  searchterm_id
) 
;

-- DROP TABLE searchterm_obj CASCADE CONSTRAINTS;


PROMPT Creating Table searchterm_obj ...
CREATE TABLE searchterm_obj (
  id NUMBER(24,0) NOT NULL,
  version NUMBER(10,0) DEFAULT '0' NOT NULL,
  obj_id NUMBER(24,0) NOT NULL,
  line NUMBER(10,0) DEFAULT '0' NOT NULL,
  searchterm_id NUMBER(24,0)
);


PROMPT Creating Primary Key Constraint PRIMARY_18 on table searchterm_obj ... 
ALTER TABLE searchterm_obj
ADD CONSTRAINT PRIMARY_18 PRIMARY KEY
(
  id
)
ENABLE
;

-- DROP TABLE searchterm_sns CASCADE CONSTRAINTS;


PROMPT Creating Table searchterm_sns ...
CREATE TABLE searchterm_sns (
  id NUMBER(24,0) NOT NULL,
  version NUMBER(10,0) DEFAULT '0' NOT NULL,
  sns_id VARCHAR2(255 CHAR),
  expired_at VARCHAR2(17 CHAR)
);


PROMPT Creating Primary Key Constraint PRIMARY_19 on table searchterm_sns ... 
ALTER TABLE searchterm_sns
ADD CONSTRAINT PRIMARY_19 PRIMARY KEY
(
  id
)
ENABLE
;
PROMPT Creating Index idxTermSnsId on searchterm_sns ...
CREATE INDEX idxTermSnsId ON searchterm_sns
(
  sns_id
) 
;

-- DROP TABLE spatial_ref_sns CASCADE CONSTRAINTS;


PROMPT Creating Table spatial_ref_sns ...
CREATE TABLE spatial_ref_sns (
  id NUMBER(24,0) NOT NULL,
  version NUMBER(10,0) DEFAULT '0' NOT NULL,
  sns_id VARCHAR2(255 CHAR),
  expired_at VARCHAR2(17 CHAR)
);


PROMPT Creating Primary Key Constraint PRIMARY_21 on table spatial_ref_sns ... 
ALTER TABLE spatial_ref_sns
ADD CONSTRAINT PRIMARY_21 PRIMARY KEY
(
  id
)
ENABLE
;
PROMPT Creating Index idxSpatialSnsId on spatial_ref_sns ...
CREATE INDEX idxSpatialSnsId ON spatial_ref_sns
(
  sns_id
) 
;

-- DROP TABLE spatial_ref_value CASCADE CONSTRAINTS;


PROMPT Creating Table spatial_ref_value ...
CREATE TABLE spatial_ref_value (
  id NUMBER(24,0) NOT NULL,
  version NUMBER(10,0) DEFAULT '0' NOT NULL,
  type CHAR(1 CHAR),
  spatial_ref_sns_id NUMBER(24,0),
  name_key NUMBER(10,0),
  name_value VARCHAR2(4000 CHAR),
  nativekey VARCHAR2(255 CHAR),
  x1 FLOAT,
  y1 FLOAT,
  x2 FLOAT,
  y2 FLOAT
);


PROMPT Creating Primary Key Constraint PRIMARY_22 on table spatial_ref_value ... 
ALTER TABLE spatial_ref_value
ADD CONSTRAINT PRIMARY_22 PRIMARY KEY
(
  id
)
ENABLE
;
PROMPT Creating Index idxSRVal_SRefSNSId on spatial_ref_value ...
CREATE INDEX idxSRVal_SRefSNSId ON spatial_ref_value
(
  spatial_ref_sns_id
) 
;

-- DROP TABLE spatial_reference CASCADE CONSTRAINTS;


PROMPT Creating Table spatial_reference ...
CREATE TABLE spatial_reference (
  id NUMBER(24,0) NOT NULL,
  version NUMBER(10,0) DEFAULT '0' NOT NULL,
  obj_id NUMBER(24,0) NOT NULL,
  line NUMBER(10,0) DEFAULT '0' NOT NULL,
  spatial_ref_id NUMBER(24,0)
);


PROMPT Creating Primary Key Constraint PRIMARY_23 on table spatial_reference ... 
ALTER TABLE spatial_reference
ADD CONSTRAINT PRIMARY_23 PRIMARY KEY
(
  id
)
ENABLE
;
PROMPT Creating Index idxSRef_ObjId on spatial_reference ...
CREATE INDEX idxSRef_ObjId ON spatial_reference
(
  obj_id
) 
;
PROMPT Creating Index idxSRef_SRefId on spatial_reference ...
CREATE INDEX idxSRef_SRefId ON spatial_reference
(
  spatial_ref_id
) 
;

-- DROP TABLE sys_list CASCADE CONSTRAINTS;


PROMPT Creating Table sys_list ...
CREATE TABLE sys_list (
  id NUMBER(24,0) NOT NULL,
  version NUMBER(10,0) DEFAULT '0' NOT NULL,
  lst_id NUMBER(10,0) DEFAULT '0' NOT NULL,
  entry_id NUMBER(10,0) DEFAULT '0' NOT NULL,
  lang_id CHAR(2 CHAR) NOT NULL,
  name VARCHAR2(255 CHAR),
  description VARCHAR2(255 CHAR),
  maintainable NUMBER(10,0) DEFAULT '0',
  is_default CHAR(1 CHAR) DEFAULT 'N'
);


PROMPT Creating Primary Key Constraint PRIMARY_27 on table sys_list ... 
ALTER TABLE sys_list
ADD CONSTRAINT PRIMARY_27 PRIMARY KEY
(
  id
)
ENABLE
;
PROMPT Creating Index entry_id on sys_list ...
CREATE INDEX entry_id ON sys_list
(
  entry_id,
  lst_id,
  lang_id
) 
;
PROMPT Creating Index idxSysList_LstId on sys_list ...
CREATE INDEX idxSysList_LstId ON sys_list
(
  lst_id
) 
;

-- DROP TABLE sys_generic_key CASCADE CONSTRAINTS;


PROMPT Creating Table sys_generic_key ...
CREATE TABLE sys_generic_key (
  key_name VARCHAR2(255 CHAR) NOT NULL,
  value_string VARCHAR2(255 CHAR)
);


PROMPT Creating Unique Index key_name on sys_generic_key...
CREATE UNIQUE INDEX key_name ON sys_generic_key
(
  key_name
) 
;

-- DROP TABLE t0110_avail_format CASCADE CONSTRAINTS;


PROMPT Creating Table t0110_avail_format ...
CREATE TABLE t0110_avail_format (
  id NUMBER(24,0) NOT NULL,
  version NUMBER(10,0) DEFAULT '0' NOT NULL,
  obj_id NUMBER(24,0),
  line NUMBER(10,0) DEFAULT '0',
  format_value VARCHAR2(255 CHAR),
  format_key NUMBER(10,0),
  ver VARCHAR2(255 CHAR),
  file_decompression_technique VARCHAR2(255 CHAR),
  specification VARCHAR2(255 CHAR)
);


PROMPT Creating Primary Key Constraint PRIMARY_28 on table t0110_avail_format ... 
ALTER TABLE t0110_avail_format
ADD CONSTRAINT PRIMARY_28 PRIMARY KEY
(
  id
)
ENABLE
;
PROMPT Creating Index idxAvFormat_ObjId on t0110_avail_format ...
CREATE INDEX idxAvFormat_ObjId ON t0110_avail_format
(
  obj_id
) 
;

-- DROP TABLE t0112_media_option CASCADE CONSTRAINTS;


PROMPT Creating Table t0112_media_option ...
CREATE TABLE t0112_media_option (
  id NUMBER(24,0) NOT NULL,
  version NUMBER(10,0) DEFAULT '0' NOT NULL,
  obj_id NUMBER(24,0),
  line NUMBER(10,0) DEFAULT '0',
  medium_note VARCHAR2(255 CHAR),
  medium_name NUMBER(10,0),
  transfer_size FLOAT
);


PROMPT Creating Primary Key Constraint PRIMARY_29 on table t0112_media_option ... 
ALTER TABLE t0112_media_option
ADD CONSTRAINT PRIMARY_29 PRIMARY KEY
(
  id
)
ENABLE
;
PROMPT Creating Index idxMediaOp_ObjId on t0112_media_option ...
CREATE INDEX idxMediaOp_ObjId ON t0112_media_option
(
  obj_id
) 
;

-- DROP TABLE t0113_dataset_reference CASCADE CONSTRAINTS;


PROMPT Creating Table t0113_dataset_reference ...
CREATE TABLE t0113_dataset_reference (
  id NUMBER(24,0) NOT NULL,
  version NUMBER(10,0) DEFAULT '0' NOT NULL,
  obj_id NUMBER(24,0),
  line NUMBER(10,0) DEFAULT '0',
  reference_date VARCHAR2(17 CHAR),
  type NUMBER(10,0)
);


PROMPT Creating Primary Key Constraint PRIMARY_30 on table t0113_dataset_reference ... 
ALTER TABLE t0113_dataset_reference
ADD CONSTRAINT PRIMARY_30 PRIMARY KEY
(
  id
)
ENABLE
;
PROMPT Creating Index idxDatRef_ObjId on t0113_dataset_reference ...
CREATE INDEX idxDatRef_ObjId ON t0113_dataset_reference
(
  obj_id
) 
;

-- DROP TABLE t0114_env_category CASCADE CONSTRAINTS;


PROMPT Creating Table t0114_env_category ...
CREATE TABLE t0114_env_category (
  id NUMBER(24,0) NOT NULL,
  version NUMBER(10,0) DEFAULT '0' NOT NULL,
  obj_id NUMBER(24,0) NOT NULL,
  line NUMBER(10,0),
  cat_key NUMBER(10,0)
);


PROMPT Creating Primary Key Constraint PRIMARY_31 on table t0114_env_category ... 
ALTER TABLE t0114_env_category
ADD CONSTRAINT PRIMARY_31 PRIMARY KEY
(
  id
)
ENABLE
;
PROMPT Creating Index idxEnvCat_obj_id on t0114_env_category ...
CREATE INDEX idxEnvCat_obj_id ON t0114_env_category
(
  obj_id
) 
;

-- DROP TABLE t0114_env_topic CASCADE CONSTRAINTS;


PROMPT Creating Table t0114_env_topic ...
CREATE TABLE t0114_env_topic (
  id NUMBER(24,0) NOT NULL,
  version NUMBER(10,0) DEFAULT '0' NOT NULL,
  obj_id NUMBER(24,0) NOT NULL,
  line NUMBER(10,0),
  topic_key NUMBER(10,0)
);


PROMPT Creating Primary Key Constraint PRIMARY_32 on table t0114_env_topic ... 
ALTER TABLE t0114_env_topic
ADD CONSTRAINT PRIMARY_32 PRIMARY KEY
(
  id
)
ENABLE
;
PROMPT Creating Index idxEnvTop_ObjId on t0114_env_topic ...
CREATE INDEX idxEnvTop_ObjId ON t0114_env_topic
(
  obj_id
) 
;

-- DROP TABLE t011_obj_data CASCADE CONSTRAINTS;


PROMPT Creating Table t011_obj_data ...
CREATE TABLE t011_obj_data (
  id NUMBER(24,0) NOT NULL,
  version NUMBER(10,0) DEFAULT '0' NOT NULL,
  obj_id NUMBER(24,0),
  base VARCHAR2(255 CHAR),
  description CLOB
);


PROMPT Creating Primary Key Constraint PRIMARY_33 on table t011_obj_data ... 
ALTER TABLE t011_obj_data
ADD CONSTRAINT PRIMARY_33 PRIMARY KEY
(
  id
)
ENABLE
;
PROMPT Creating Index idxOData_ObjId on t011_obj_data ...
CREATE INDEX idxOData_ObjId ON t011_obj_data
(
  obj_id
) 
;

-- DROP TABLE t011_obj_geo_spatial_rep CASCADE CONSTRAINTS;


PROMPT Creating Table t011_obj_geo_spatial_rep ...
CREATE TABLE t011_obj_geo_spatial_rep (
  id NUMBER(24,0) NOT NULL,
  version NUMBER(10,0) DEFAULT '0' NOT NULL,
  obj_geo_id NUMBER(24,0),
  line NUMBER(10,0) DEFAULT '0',
  type NUMBER(10,0)
);


PROMPT Creating Primary Key Constraint PRIMARY_38 on table t011_obj_geo_spatial_rep ... 
ALTER TABLE t011_obj_geo_spatial_rep
ADD CONSTRAINT PRIMARY_38 PRIMARY KEY
(
  id
)
ENABLE
;
PROMPT Creating Index idxOGeoSpat_OGeoId on t011_obj_geo_spatial_rep ...
CREATE INDEX idxOGeoSpat_OGeoId ON t011_obj_geo_spatial_rep
(
  obj_geo_id
) 
;

-- DROP TABLE t011_obj_project CASCADE CONSTRAINTS;


PROMPT Creating Table t011_obj_project ...
CREATE TABLE t011_obj_project (
  id NUMBER(24,0) NOT NULL,
  version NUMBER(10,0) DEFAULT '0' NOT NULL,
  obj_id NUMBER(24,0),
  leader VARCHAR2(255 CHAR),
  member VARCHAR2(255 CHAR),
  description CLOB
);


PROMPT Creating Primary Key Constraint PRIMARY_43 on table t011_obj_project ... 
ALTER TABLE t011_obj_project
ADD CONSTRAINT PRIMARY_43 PRIMARY KEY
(
  id
)
ENABLE
;
PROMPT Creating Index idxOProj_ObjId on t011_obj_project ...
CREATE INDEX idxOProj_ObjId ON t011_obj_project
(
  obj_id
) 
;

-- DROP TABLE t011_obj_serv_op_platform CASCADE CONSTRAINTS;


PROMPT Creating Table t011_obj_serv_op_platform ...
CREATE TABLE t011_obj_serv_op_platform (
  id NUMBER(24,0) NOT NULL,
  version NUMBER(10,0) DEFAULT '0' NOT NULL,
  obj_serv_op_id NUMBER(24,0),
  line NUMBER(10,0) DEFAULT '0',
  platform VARCHAR2(255 CHAR)
);


PROMPT Creating Primary Key Constraint PRIMARY_48 on table t011_obj_serv_op_platform ... 
ALTER TABLE t011_obj_serv_op_platform
ADD CONSTRAINT PRIMARY_48 PRIMARY KEY
(
  id
)
ENABLE
;
PROMPT Creating Index idxOSerOPl_OSerOId on t011_obj_serv_op_platform ...
CREATE INDEX idxOSerOPl_OSerOId ON t011_obj_serv_op_platform
(
  obj_serv_op_id
) 
;

-- DROP TABLE t08_attr CASCADE CONSTRAINTS;


PROMPT Creating Table t08_attr ...
CREATE TABLE t08_attr (
  id NUMBER(24,0) NOT NULL,
  version NUMBER(10,0) DEFAULT '0' NOT NULL,
  attr_type_id NUMBER(24,0) NOT NULL,
  obj_id NUMBER(24,0) NOT NULL,
  data VARCHAR2(255 CHAR)
);


PROMPT Creating Primary Key Constraint PRIMARY_62 on table t08_attr ... 
ALTER TABLE t08_attr
ADD CONSTRAINT PRIMARY_62 PRIMARY KEY
(
  id
)
ENABLE
;
PROMPT Creating Index idxAttr_AttrTypeId on t08_attr ...
CREATE INDEX idxAttr_AttrTypeId ON t08_attr
(
  attr_type_id
) 
;
PROMPT Creating Index idxAttr_ObjId on t08_attr ...
CREATE INDEX idxAttr_ObjId ON t08_attr
(
  obj_id
) 
;

-- DROP TABLE t08_attr_type CASCADE CONSTRAINTS;


PROMPT Creating Table t08_attr_type ...
CREATE TABLE t08_attr_type (
  id NUMBER(24,0) NOT NULL,
  version NUMBER(10,0) DEFAULT '0' NOT NULL,
  name VARCHAR2(255 CHAR),
  length NUMBER(10,0),
  type CHAR(1 CHAR)
);


PROMPT Creating Primary Key Constraint PRIMARY_2 on table t08_attr_type ... 
ALTER TABLE t08_attr_type
ADD CONSTRAINT PRIMARY_2 PRIMARY KEY
(
  id
)
ENABLE
;

-- DROP TABLE searchterm_value CASCADE CONSTRAINTS;


PROMPT Creating Table searchterm_value ...
CREATE TABLE searchterm_value (
  id NUMBER(24,0) NOT NULL,
  version NUMBER(10,0) DEFAULT '0' NOT NULL,
  type CHAR(1 CHAR),
  searchterm_sns_id NUMBER(24,0),
  term VARCHAR2(4000 CHAR)
);


PROMPT Creating Primary Key Constraint PRIMARY_20 on table searchterm_value ... 
ALTER TABLE searchterm_value
ADD CONSTRAINT PRIMARY_20 PRIMARY KEY
(
  id
)
ENABLE
;
PROMPT Creating Index idxSTVal_STSNSId on searchterm_value ...
CREATE INDEX idxSTVal_STSNSId ON searchterm_value
(
  searchterm_sns_id
) 
;

-- DROP TABLE t011_obj_serv_op_depends CASCADE CONSTRAINTS;


PROMPT Creating Table t011_obj_serv_op_depends ...
CREATE TABLE t011_obj_serv_op_depends (
  id NUMBER(24,0) NOT NULL,
  version NUMBER(10,0) DEFAULT '0' NOT NULL,
  obj_serv_op_id NUMBER(24,0),
  line NUMBER(10,0) DEFAULT '0',
  depends_on VARCHAR2(255 CHAR)
);


PROMPT Creating Primary Key Constraint PRIMARY_46 on table t011_obj_serv_op_depends ... 
ALTER TABLE t011_obj_serv_op_depends
ADD CONSTRAINT PRIMARY_46 PRIMARY KEY
(
  id
)
ENABLE
;
PROMPT Creating Index idxOserODe_OSerOId on t011_obj_serv_op_depends ...
CREATE INDEX idxOserODe_OSerOId ON t011_obj_serv_op_depends
(
  obj_serv_op_id
) 
;

-- DROP TABLE t011_obj_serv_op_para CASCADE CONSTRAINTS;


PROMPT Creating Table t011_obj_serv_op_para ...
CREATE TABLE t011_obj_serv_op_para (
  id NUMBER(24,0) NOT NULL,
  version NUMBER(10,0) DEFAULT '0' NOT NULL,
  obj_serv_op_id NUMBER(24,0),
  line NUMBER(10,0) DEFAULT '0',
  name VARCHAR2(255 CHAR),
  direction VARCHAR2(255 CHAR),
  descr CLOB,
  optional NUMBER(10,0),
  repeatability NUMBER(10,0)
);


PROMPT Creating Primary Key Constraint PRIMARY_47 on table t011_obj_serv_op_para ... 
ALTER TABLE t011_obj_serv_op_para
ADD CONSTRAINT PRIMARY_47 PRIMARY KEY
(
  id
)
ENABLE
;
PROMPT Creating Index idxOSerOPa_OSerOId on t011_obj_serv_op_para ...
CREATE INDEX idxOSerOPa_OSerOId ON t011_obj_serv_op_para
(
  obj_serv_op_id
) 
;

-- DROP TABLE sys_export CASCADE CONSTRAINTS;

PROMPT Creating Table sys_export ...
CREATE TABLE sys_export
(
	export_id NUMBER(24,0) NOT NULL,
	num_objects NUMBER(10,0),
	num_addresses NUMBER(10,0),
	exporttime VARCHAR2(17 CHAR)
);
