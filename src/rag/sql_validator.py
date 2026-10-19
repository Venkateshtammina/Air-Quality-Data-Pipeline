
import sqlglot
from sqlglot import exp


ALLOWED_TABLES = {
    "fact_air_quality",
    "dim_date",
    "dim_time",
    "dim_station",
}

ALLOWED_ANONYMOUS_FUNCTIONS = set()


def validate_sql(sql: str) -> str:
    """Validate a single read-only query against the allowed schema."""

    if not isinstance(sql, str) or not sql.strip():
        raise ValueError("SQL query must not be empty.")

    try:
        statements = sqlglot.parse(sql, read="postgres")
    except sqlglot.errors.ParseError as exc:
        raise ValueError(
            "Generated SQL is not valid PostgreSQL syntax."
        ) from exc

    # Reject multiple statements and empty input.
    if len(statements) != 1 or statements[0] is None:
        raise ValueError("Exactly one SQL statement is allowed.")

    query = statements[0]

    # Allow SELECT queries and set operations, including WITH ... SELECT.
    main_query = query

    while isinstance(main_query, (exp.Subquery, exp.Paren)):
        main_query = main_query.this

    if not isinstance(
        main_query,
        (exp.Select, exp.Union, exp.Intersect, exp.Except),
    ):
        raise ValueError("Only SELECT queries are allowed.")

    # Reject data-changing and other potentially unsafe SQL constructs.
    forbidden = (
        exp.Insert,
        exp.Update,
        exp.Delete,
        exp.Drop,
        exp.Alter,
        exp.Create,
        exp.Command,
        exp.Merge,
        exp.Transaction,
        exp.Copy,
        exp.Pragma,
    )

    for node in query.walk():
        if isinstance(node, forbidden):
            raise ValueError(
                "The query contains a prohibited SQL operation."
            )

        if isinstance(node, exp.Anonymous):
            function_name = node.name.lower()
            if function_name not in ALLOWED_ANONYMOUS_FUNCTIONS:
                raise ValueError(
                    f"Function '{node.name}' is not permitted."
                )

    # Collect CTE names so references to them aren't mistaken
    # for unapproved physical database tables.
    cte_names = {
        cte.alias_or_name.lower()
        for cte in query.find_all(exp.CTE)
        if cte.alias_or_name
    }
    cte_self_references = {
        cte.alias_or_name.lower()
        for cte in query.find_all(exp.CTE)
        if cte.alias_or_name
        and any(
            table.name.lower() == cte.alias_or_name.lower()
            for table in cte.this.find_all(exp.Table)
        )
    }

    # Restrict physical table references to approved public tables.
    for table in query.find_all(exp.Table):
        table_name = table.name.lower()

        refers_to_own_cte = table_name in cte_self_references
        if (table_name in cte_names and not refers_to_own_cte
                and not table.db and not table.catalog):
            continue

        if table_name not in ALLOWED_TABLES:
            raise ValueError(
                f"Table '{table.name}' is not permitted."
            )

        if table.db and table.db.lower() != "public":
            raise ValueError(
                "Only tables in the public schema are allowed."
            )

        if table.catalog:
            raise ValueError(
                "Cross-database references are not allowed."
            )

    return query.sql(dialect="postgres")
