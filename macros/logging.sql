{% macro log_model_run(source_relations=[], bad_record_conditions=[], source_relation=None, bad_record_source=None) %}
    {% set model_name = this.identifier %}
    {% set run_id = invocation_id %}

    {% if source_relation is not none and source_relations | length == 0 %}
        {% set source_relations = [source_relation] %}
    {% endif %}

    {# Construct source record count SQL for all source_relations #}
    {% set source_count_queries = [] %}
    {% for relation in source_relations %}
        {% do source_count_queries.append("SELECT COUNT(*) FROM " ~ relation) %}
    {% endfor %}
    {% if source_count_queries | length > 0 %}
        {% set source_counts_sql = "(" ~ source_count_queries | join(") + (") ~ ")" %}
    {% else %}
        {% set source_counts_sql = "NULL" %}
    {% endif %}

    {# Target record count SQL #}
    {% set target_count_sql = "SELECT COUNT(*) FROM " ~ this %}

    {# Determine where to check bad records #}
    {% set bad_source = bad_record_source if bad_record_source is not none else this %}

    {# Count bad records SQL #}
    {% if bad_record_conditions | length > 0 %}
        {% set bad_record_queries = [] %}
        {% for cond in bad_record_conditions %}
            {% do bad_record_queries.append("SELECT COUNT(*) FROM " ~ bad_source ~ " WHERE " ~ cond) %}
        {% endfor %}
        {% set bad_records_sql = "(" ~ bad_record_queries | join(") + (") ~ ")" %}
    {% else %}
        {% set bad_records_sql = "0" %}
    {% endif %}

    insert into logs.dbt_logs (
        dataset,
        time_processed,
        source_records,
        target_records,
        bad_records,
        model_name,
        run_id,
        status,
        execution_time_seconds,
        created_at
    )
    select
        '{{ model_name }}' as dataset,
        current_timestamp() as time_processed,
        ({{ source_counts_sql }}) as source_records,
        ({{ target_count_sql }}) as target_records,
        ({{ bad_records_sql }}) as bad_records,
        '{{ model_name }}' as model_name,
        '{{ run_id }}' as run_id,
        'SUCCESS' as status,
        NULL as execution_time_seconds,
        current_timestamp() as created_at
{% endmacro %}
