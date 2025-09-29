LOAD LABEL {{ database_name }}.{{load_label}}
(
    DATA INFILE %(tsv_filepath)s
    INTO TABLE {{table_name}}
    COLUMNS TERMINATED BY "\t"
    FORMAT AS "CSV"
    (chromosome, start, end, cytoband, gie_stain)
    SET
    (
        chromosome = replace(chromosome, 'chr', ''),
        start = start,
        end = end,
        cytoband = cytoband,
        gie_stain = gie_stain
     )
     where chromosome in (
        '1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11',
        '12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '22',
        'X', 'Y', 'M'
     )

 )
 WITH BROKER
 (
        {{ broker_configuration }}
 )
PROPERTIES
(
    'timeout' = '{{ broker_load_timeout }}'
);