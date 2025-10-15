from dbt_multi_adapter_utils.jinja_parser import JinjaTemplate


def test_jinja_template_handles_pure_sql():
    template_text = "SELECT * FROM users WHERE created_at > '2024-01-01'"
    template = JinjaTemplate(template_text)
    result = template.can_safely_rewrite()

    assert result.can_rewrite


def test_jinja_template_identifies_safe_expressions():
    template_text = "SELECT * FROM {{ ref('users') }} WHERE id = {{ var('user_id') }}"
    template = JinjaTemplate(template_text)
    safe_regions = template.extract_safe_sql_regions()

    assert len(safe_regions) > 0
    assert "SELECT" in safe_regions[0].masked_sql


def test_jinja_template_detects_control_flow():
    template_text = """
    SELECT *
    FROM users
    {% if include_deleted %}
    WHERE deleted_at IS NOT NULL
    {% endif %}
    """
    template = JinjaTemplate(template_text)
    result = template.can_safely_rewrite()

    # Control flow is now supported - should be safe to rewrite
    assert result.can_rewrite
    assert "safe" in result.reason.lower()


def test_jinja_template_extracts_safe_regions():
    template_text = "SELECT DATE_TRUNC('month', created_at) FROM {{ ref('users') }}"
    template = JinjaTemplate(template_text)
    safe_regions = template.extract_safe_sql_regions()

    assert len(safe_regions) > 0
    combined_sql = "".join(region.masked_sql for region in safe_regions)
    assert "DATE_TRUNC" in combined_sql


def test_jinja_template_handles_complex_jinja():
    template_text = """
    SELECT
      {% if use_date_trunc %}
        DATE_TRUNC('month', created_at)
      {% else %}
        created_at
      {% endif %}
    FROM users
    """
    template = JinjaTemplate(template_text)
    result = template.can_safely_rewrite()

    # Control flow with if/else is now supported
    assert result.can_rewrite


def test_jinja_template_approves_simple_template():
    template_text = "SELECT DATE_TRUNC('month', created_at) FROM {{ ref('users') }} WHERE id = 1"
    template = JinjaTemplate(template_text)
    result = template.can_safely_rewrite()

    assert result.can_rewrite
    assert "safe" in result.reason.lower()


def test_jinja_template_handles_invalid_jinja():
    template_text = "SELECT * FROM {{ ref('users' }}"
    template = JinjaTemplate(template_text)
    result = template.can_safely_rewrite()

    assert not result.can_rewrite


def test_jinja_template_handles_empty_template():
    template_text = ""
    template = JinjaTemplate(template_text)
    result = template.can_safely_rewrite()

    assert result.can_rewrite
