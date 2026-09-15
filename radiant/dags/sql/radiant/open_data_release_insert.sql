INSERT INTO {{ mapping.starrocks_open_data_release }}
    (table_name, recorded_at, dag_run_id, catalog_name, database_name, iceberg_ref, dataset_version)
VALUES
{% for r in releases %}
    ('{{ r.table_name }}', '{{ recorded_at }}', '{{ dag_run_id }}', '{{ r.catalog_name }}', '{{ r.database_name }}',
     {% if r.iceberg_ref %}'{{ r.iceberg_ref }}'{% else %}NULL{% endif %},
     {% if r.dataset_version %}'{{ r.dataset_version }}'{% else %}NULL{% endif %}){% if not loop.last %},{% endif %}
{% endfor %}
;
