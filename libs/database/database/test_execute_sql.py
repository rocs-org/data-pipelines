from .execute_sql import snake_case_to_camel_case, camel_case_to_snake_case


def test_snake_case_to_camel_case():
    assert snake_case_to_camel_case("snake_case") == "SnakeCase"


def test_camel_case_to_snake_case():
    assert camel_case_to_snake_case("CamelCase") == "camel_case"
    assert camel_case_to_snake_case("snake_case") == "snake_case"
