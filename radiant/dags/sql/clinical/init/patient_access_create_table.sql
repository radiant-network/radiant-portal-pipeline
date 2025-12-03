CREATE TABLE IF NOT EXISTS {{ mapping.starrocks_patient_access }} (
    'id' varchar(500) NULL COMMENT "",
    'mrn' varchar(128) NULL COMMENT "",
    'gender' varchar(128) NULL COMMENT "", 
    'race' varchar(128) NULL COMMENT "",
    'ethnicity' varchar(128) NULL COMMENT "",
    'given_name' varchar(128) NULL COMMENT "",
    'family_name' varchar(128) NULL COMMENT "",
    'birth_date' varchar(128) NULL COMMENT "",
    'deceased_boolean' varchar(128) NULL COMMENT "",
    'address_postal_code' varchar(128) NULL COMMENT ""
    ) ENGINE=OLAP
   DUPLICATE KEY(`id`); --is this the right way to specify the field that acts as primary key? 