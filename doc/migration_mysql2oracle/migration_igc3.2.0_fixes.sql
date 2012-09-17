PROMPT ! CHANGE COLUMN DATA TYPES IGC AFTER MIGRATION VIA SQL DEVELOPER !
PROMPT --------------------------------------------------------------

PROMPT ! Change t02_address.institution from CLOB to VARCHAR2(4000 CHAR) ...
ALTER TABLE t02_address ADD institution2 VARCHAR2(4000 CHAR);
UPDATE t02_address SET institution2 = institution;
ALTER TABLE t02_address DROP COLUMN institution;
ALTER TABLE t02_address RENAME COLUMN institution2 TO institution;

PROMPT ! Change object_conformity.specification_value from CLOB to VARCHAR2(4000 CHAR) ...
ALTER TABLE object_conformity ADD specification_value2 VARCHAR2(4000 CHAR);
UPDATE object_conformity SET specification_value2 = specification_value;
ALTER TABLE object_conformity DROP COLUMN specification_value;
ALTER TABLE object_conformity RENAME COLUMN specification_value2 TO specification_value;

PROMPT ! Change object_use.terms_of_use_value from CLOB to VARCHAR2(4000 CHAR) ...
ALTER TABLE object_use ADD terms_of_use_value2 VARCHAR2(4000 CHAR);
UPDATE object_use SET terms_of_use_value2 = terms_of_use_value;
ALTER TABLE object_use DROP COLUMN terms_of_use_value;
ALTER TABLE object_use RENAME COLUMN terms_of_use_value2 TO terms_of_use_value;

PROMPT ! Change searchterm_value.term from CLOB to VARCHAR2(4000 CHAR) ...
ALTER TABLE searchterm_value ADD term2 VARCHAR2(4000 CHAR);
UPDATE searchterm_value SET term2 = term;
ALTER TABLE searchterm_value DROP COLUMN term;
ALTER TABLE searchterm_value RENAME COLUMN term2 TO term;

PROMPT ! Change searchterm_value.alternate_term from CLOB to VARCHAR2(4000 CHAR) ...
ALTER TABLE searchterm_value ADD alternate_term2 VARCHAR2(4000 CHAR);
UPDATE searchterm_value SET alternate_term2 = alternate_term;
ALTER TABLE searchterm_value DROP COLUMN alternate_term;
ALTER TABLE searchterm_value RENAME COLUMN alternate_term2 TO alternate_term;

PROMPT ! Change spatial_ref_value.name_value from CLOB to VARCHAR2(4000 CHAR) ...
ALTER TABLE spatial_ref_value ADD name_value2 VARCHAR2(4000 CHAR);
UPDATE spatial_ref_value SET name_value2 = name_value;
ALTER TABLE spatial_ref_value DROP COLUMN name_value;
ALTER TABLE spatial_ref_value RENAME COLUMN name_value2 TO name_value;

PROMPT ! Change sys_list.name from CLOB to VARCHAR2(4000 CHAR) ...
ALTER TABLE sys_list ADD name2 VARCHAR2(4000 CHAR);
UPDATE sys_list SET name2 = name;
ALTER TABLE sys_list DROP COLUMN name;
ALTER TABLE sys_list RENAME COLUMN name2 TO name;

PROMPT ! Change sys_list.description from CLOB to VARCHAR2(4000 CHAR) ...
ALTER TABLE sys_list ADD description2 VARCHAR2(4000 CHAR);
UPDATE sys_list SET description2 = description;
ALTER TABLE sys_list DROP COLUMN description;
ALTER TABLE sys_list RENAME COLUMN description2 TO description;

PROMPT ! Change t011_obj_data.base from CLOB to VARCHAR2(4000 CHAR) ...
ALTER TABLE t011_obj_data ADD base2 VARCHAR2(4000 CHAR);
UPDATE t011_obj_data SET base2 = base;
ALTER TABLE t011_obj_data DROP COLUMN base;
ALTER TABLE t011_obj_data RENAME COLUMN base2 TO base;

PROMPT ! Change t011_obj_geo_supplinfo.feature_type from CLOB to VARCHAR2(4000 CHAR) ...
ALTER TABLE t011_obj_geo_supplinfo ADD feature_type2 VARCHAR2(4000 CHAR);
UPDATE t011_obj_geo_supplinfo SET feature_type2 = feature_type;
ALTER TABLE t011_obj_geo_supplinfo DROP COLUMN feature_type;
ALTER TABLE t011_obj_geo_supplinfo RENAME COLUMN feature_type2 TO feature_type;

PROMPT ! Change t011_obj_literature.author from CLOB to VARCHAR2(4000 CHAR) ...
ALTER TABLE t011_obj_literature ADD author2 VARCHAR2(4000 CHAR);
UPDATE t011_obj_literature SET author2 = author;
ALTER TABLE t011_obj_literature DROP COLUMN author;
ALTER TABLE t011_obj_literature RENAME COLUMN author2 TO author;

PROMPT ! Change t011_obj_literature.publisher from CLOB to VARCHAR2(4000 CHAR) ...
ALTER TABLE t011_obj_literature ADD publisher2 VARCHAR2(4000 CHAR);
UPDATE t011_obj_literature SET publisher2 = publisher;
ALTER TABLE t011_obj_literature DROP COLUMN publisher;
ALTER TABLE t011_obj_literature RENAME COLUMN publisher2 TO publisher;

PROMPT ! Change t011_obj_literature.publish_in from CLOB to VARCHAR2(4000 CHAR) ...
ALTER TABLE t011_obj_literature ADD publish_in2 VARCHAR2(4000 CHAR);
UPDATE t011_obj_literature SET publish_in2 = publish_in;
ALTER TABLE t011_obj_literature DROP COLUMN publish_in;
ALTER TABLE t011_obj_literature RENAME COLUMN publish_in2 TO publish_in;

PROMPT ! Change t011_obj_literature.publish_loc from CLOB to VARCHAR2(4000 CHAR) ...
ALTER TABLE t011_obj_literature ADD publish_loc2 VARCHAR2(4000 CHAR);
UPDATE t011_obj_literature SET publish_loc2 = publish_loc;
ALTER TABLE t011_obj_literature DROP COLUMN publish_loc;
ALTER TABLE t011_obj_literature RENAME COLUMN publish_loc2 TO publish_loc;

PROMPT ! Change t011_obj_literature.loc from CLOB to VARCHAR2(4000 CHAR) ...
ALTER TABLE t011_obj_literature ADD loc2 VARCHAR2(4000 CHAR);
UPDATE t011_obj_literature SET loc2 = loc;
ALTER TABLE t011_obj_literature DROP COLUMN loc;
ALTER TABLE t011_obj_literature RENAME COLUMN loc2 TO loc;

PROMPT ! Change t011_obj_literature.doc_info from CLOB to VARCHAR2(4000 CHAR) ...
ALTER TABLE t011_obj_literature ADD doc_info2 VARCHAR2(4000 CHAR);
UPDATE t011_obj_literature SET doc_info2 = doc_info;
ALTER TABLE t011_obj_literature DROP COLUMN doc_info;
ALTER TABLE t011_obj_literature RENAME COLUMN doc_info2 TO doc_info;

PROMPT ! Change t011_obj_literature.publishing from CLOB to VARCHAR2(4000 CHAR) ...
ALTER TABLE t011_obj_literature ADD publishing2 VARCHAR2(4000 CHAR);
UPDATE t011_obj_literature SET publishing2 = publishing;
ALTER TABLE t011_obj_literature DROP COLUMN publishing;
ALTER TABLE t011_obj_literature RENAME COLUMN publishing2 TO publishing;

PROMPT ! Change t011_obj_project.leader from CLOB to VARCHAR2(4000 CHAR) ...
ALTER TABLE t011_obj_project ADD leader2 VARCHAR2(4000 CHAR);
UPDATE t011_obj_project SET leader2 = leader;
ALTER TABLE t011_obj_project DROP COLUMN leader;
ALTER TABLE t011_obj_project RENAME COLUMN leader2 TO leader;

PROMPT ! Change t011_obj_project.member from CLOB to VARCHAR2(4000 CHAR) ...
ALTER TABLE t011_obj_project ADD member2 VARCHAR2(4000 CHAR);
UPDATE t011_obj_project SET member2 = member;
ALTER TABLE t011_obj_project DROP COLUMN member;
ALTER TABLE t011_obj_project RENAME COLUMN member2 TO member;

commit;

PROMPT ! CHANGE COLUMN DATA TYPES IGC AFTER MIGRATION VIA SQL DEVELOPER !
PROMPT ! DONE !
