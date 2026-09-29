INSERT INTO {{ mapping.starrocks_open_data_release }}
    (source_name, recorded_at, dag_run_id, table_name, catalog_name, database_name, iceberg_ref, dataset_version, imported_snapshot_id, reannotated_snapshot_id)
VALUES
{% for r in releases %}
    ('{{ r.source_name }}', NOW(), '{{ dag_run_id }}', '{{ r.table_name }}',
     '{{ r.catalog_name }}', '{{ r.database_name }}',
     {% if r.iceberg_ref %}'{{ r.iceberg_ref }}'{% else %}NULL{% endif %},
     {% if r.dataset_version %}'{{ r.dataset_version }}'{% else %}NULL{% endif %},
     {% if r.imported_snapshot_id %}{{ r.imported_snapshot_id }}{% else %}NULL{% endif %},
     {% if r.reannotated_snapshot_id %}{{ r.reannotated_snapshot_id }}{% else %}NULL{% endif %}){% if not loop.last %},{% endif %}
{% endfor %}
;
