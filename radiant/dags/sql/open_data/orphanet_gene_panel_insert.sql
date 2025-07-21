INSERT OVERWRITE {{ params.starrocks_orphanet_gene_panel }}
SELECT gene_symbol as symbol,
       name as panel,
       disorder_id,
       type_of_inheritance as type_of_inheritance,
       array_filter(
               array_map(type_of_inheritance, x->
                                              case
                                                  when x = 'Autosomal recessive' THEN 'AR'
                                                  when x = 'Autosomal dominant' THEN 'AD'
                                                  when x = 'X-linked recessive' THEN 'XL'
                                                  when x = 'X-linked dominant' THEN 'XLD'
                                                  when x = 'X-linked recessive' THEN 'XLR'
                                                  when x = 'Mitochondrial inheritance' THEN 'Mi'
                                                  when x = 'Multigenic/multifactorial' THEN 'Mu'
                                                  when x = 'Oligogenic' THEN 'OG'
                                                  when x = 'Semi-dominant' THEN 'SD'
                                                  when x = 'Y-linked' THEN 'YL'
                                                  end
               ),
      x -> x is not null) as inheritance_code
FROM {{ params.iceberg_orphanet_gene_set }}
;

