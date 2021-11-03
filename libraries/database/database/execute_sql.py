from typing import List, Any
import re
import psycopg2
import ramda as R

from .types import DBContext, Cursor, DBContextWithCursor
from .db_context import (
    close_cursor,
    open_cursor,
    raiser,
)


def rollback_changes(context: DBContextWithCursor) -> DBContextWithCursor:
    return R.tap(R.pipe(R.prop("connection"), R.invoker(0, "rollback")))(context)


def commit_changes(context: DBContextWithCursor) -> DBContextWithCursor:
    return R.tap(R.pipe(R.prop("connection"), R.invoker(0, "commit")))(context)


def cleanup_db_connection(context: DBContextWithCursor) -> int:
    return R.pipe(rollback_changes, close_cursor)(context)


@R.curry
def execute_values(context: DBContext, query: str, tuples: List[Any]) -> DBContext:
    return R.unapply(
        R.pipe(
            R.tap(
                R.apply(
                    R.use_with(
                        psycopg2.extras.execute_values,
                        [R.pipe(open_cursor, R.prop("cursor")), R.identity, R.identity],
                    )
                )
            ),
            R.head,
            commit_changes,
            close_cursor,
        )
    )(context, query, tuples)


@R.curry
def execute_sql(context, sql: str, data=None) -> None:
    return R.pipe(
        R.try_catch(
            R.apply(
                R.use_with(
                    _execute_sql_on_cursor, [open_cursor, R.identity, R.identity]
                )
            ),
            R.unapply(
                R.pipe(
                    R.tap(
                        R.pipe(
                            R.nth(1),
                            R.head,
                            rollback_changes,
                        )
                    ),
                    R.head,
                    raiser,
                )
            ),
        ),
        close_cursor,
    )([context, sql, data])


@R.curry
def query_one_element(context, sql: str) -> Any:
    cursor = _execute_sql_and_return_cursor(context, sql)
    res = cursor.fetchone()[0]
    cursor.close()
    return res


@R.curry
def query_all_elements(context, sql: str) -> Any:
    cursor = _execute_sql_and_return_cursor(
        context,
        sql,
    )
    res = cursor.fetchall()
    cursor.close()
    return res


@R.curry
def _execute_sql_and_return_cursor(context: DBContext, sql: str) -> Cursor:
    return R.pipe(
        R.apply(R.use_with(_execute_sql_on_cursor, [open_cursor, R.identity])),
        R.prop("cursor"),
    )([context, sql])


@R.curry
def _execute_sql_on_cursor(
    context: DBContextWithCursor, sql: str, data=None
) -> DBContextWithCursor:
    context["cursor"].execute(sql, data)
    return context


@R.curry
def _create_database(context: DBContext) -> DBContext:
    execute_sql(
        context, f"CREATE DATABASE {R.path(['credentials', 'database'], context)};"
    )
    return context


def camel_case_to_snake_case(string: str):
    return re.sub(r"(?<!^)(?=[A-Z])", "_", string).lower()


def snake_case_to_camel_case(string: str):
    return "".join(word.title() for word in string.split("_"))
