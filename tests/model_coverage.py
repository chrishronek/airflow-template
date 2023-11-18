import os
import sys
import uuid

import yaml
from jinja2 import BaseLoader, Environment
from sqlglot import exp, parse_one
from sqlglot.dialects import Redshift
from tabulate import tabulate


def set_multiline_output(name, value):
    with open(os.environ["GITHUB_OUTPUT"], "a") as fh:
        delimiter = uuid.uuid1()
        print(f"{name}<<{delimiter}", file=fh)
        print(value, file=fh)
        print(delimiter, file=fh)


class DbtCoverage:
    def __init__(self, project_dir: str, filtered_models: list = None):
        self.project_dir = project_dir
        self.filtered_models = filtered_models

    def parse_sql(self, model_name: str, sql: str) -> str:
        """
        :param model_name: the name of the dbt model
        :param sql: The raw sql that has jinja variables
        :return:
        """

        # initialize the jinja parser
        env = Environment(loader=BaseLoader())

        # set some functions to insert placeholders or default values for dbt functions
        def use_main_arg(value):
            return value

        def null_it(**kwargs):
            return ""

        def source_function(schema, table_name):
            return f"{schema}.{table_name}"

        def is_incremental_function():
            return True

        env.globals.update(
            ref=use_main_arg,
            source=source_function,
            is_incremental=is_incremental_function,
            env_var=use_main_arg,
            dynamic_schema=use_main_arg,
            config=null_it,
        )

        # return the rendered SQL
        return env.from_string(sql).render(this=model_name)

    def get_sql_models(self):
        directory = os.path.join(self.project_dir, "models")
        sql_files = []

        # read of the .sql files in the /models directory and make a list of dictionaries for them
        for root, dirs, files in os.walk(directory):
            for file in files:
                if file.endswith(".sql"):

                    # If model isn't in filtered_models skip
                    if self.filtered_models and os.path.splitext(file)[0] not in self.filtered_models:
                        continue

                    # Use sqlglot to get model columns
                    with open(os.path.join(root, file)) as sql_file:
                        # read the raw SQL into a variable
                        sql_code = sql_file.read()

                        # parse the jinja stuff
                        rendered_sql = self.parse_sql(model_name=os.path.splitext(file)[0], sql=sql_code)

                        # gather the column names from the query
                        column_names = []
                        for expression in (
                            parse_one(rendered_sql, dialect=Redshift).find(exp.Select).args["expressions"]
                        ):
                            if isinstance(expression, exp.Alias):
                                column_names.append(expression.text("alias"))
                            elif isinstance(expression, exp.Column):
                                column_names.append(expression.text("this"))

                    # Add to the list of models
                    sql_files.append(
                        {
                            "parent": os.path.basename(root),
                            "model_name": os.path.splitext(file)[0],
                            "columns_from_sql": column_names,
                        }
                    )

        return sql_files

    def get_yml_models(self):
        directory = os.path.join(self.project_dir, "models")
        models = []
        for root, dirs, files in os.walk(directory):
            for file in files:
                if file.endswith(".yml"):
                    yaml_path = os.path.join(root, file)
                    with open(yaml_path) as yaml_file:
                        yaml_contents = yaml.safe_load(yaml_file)
                    yaml_models = yaml_contents.get("models", [])
                    for model in yaml_models:

                        # If model isn't in filtered_models skip
                        if self.filtered_models and model.get("name") not in self.filtered_models:
                            continue

                        has_table_description = model.get("description", "none_specified") != "none_specified"
                        columns = model.get("columns", [])

                        cols_descriptions = 0
                        col_tests = 0
                        for column in columns:
                            if column.get("description", "none_specified") != "none_specified":
                                cols_descriptions += 1
                            if len(column.get("tests", [])) != 0:
                                col_tests += 1

                        models.append(
                            {
                                "in_yml_meta": True,
                                "model_name": model.get("name"),
                                "has_table_description": has_table_description,
                                "columns_from_yml": [
                                    {"col_name": column.get("name"), "col_descr": column.get("description", None)}
                                    for column in columns
                                ],
                                "column_descriptions": cols_descriptions,
                                "has_column_test": col_tests > 0,
                            }
                        )

        return models

    def combine_yml_and_sql(self):
        sql_files = self.get_sql_models()
        yml_models = self.get_yml_models()

        # Create a dictionary for quick lookup based on 'name' in var_2
        yml_models_lookup = {item["model_name"]: item for item in yml_models}

        # Perform a left join
        results = []
        for item in sql_files:
            model_name = item["model_name"]
            matching_item = yml_models_lookup.get(
                model_name,
                {
                    "in_yml_meta": False,
                    "has_table_description": False,
                    "has_column_test": False,
                    "columns_from_yml": [],
                    "column_descriptions": 0,
                    "column_tests": False,
                },
            )  # Default to an empty dictionary if model is not found in yml_models
            item.update(matching_item)
            results.append(item)

        # Column comparison
        for result in results:
            columns_from_sql = result.get("columns_from_sql", [])
            columns_from_yml = [col["col_name"] for col in result.get("columns_from_yml", [])]
            columns_from_yml_w_descr = result.get("columns_from_yml", [])
            table_descriptions = 1 if result.get("has_table_description", False) is True else 0
            column_descriptions = result.get("column_descriptions", 0)
            written_descriptions = table_descriptions + column_descriptions
            expected_descriptions = len(columns_from_sql) + 1  # descriptions for all columns and table itself

            # Find descriptions for the expected SQL columns
            missing_col_descr = []
            for col in columns_from_sql:
                if col in columns_from_yml:
                    for col_w_descr in columns_from_yml_w_descr:
                        if col_w_descr.get("col_name") == col:
                            descr = col_w_descr.get("col_descr")
                            if descr is None:
                                missing_col_descr.append(col)

            # Find columns that should exist in documentation but don't
            yml_missing_cols = []
            for col in columns_from_sql:
                if col not in columns_from_yml:
                    yml_missing_cols.append(col)

            # Find columns that are documented that don't exist
            yml_extra_cols = []
            for col in columns_from_yml:
                if col not in columns_from_sql:
                    yml_extra_cols.append(col)

            # Calculate the percentage of descriptions
            description_coverage = 100 * (
                written_descriptions / expected_descriptions if expected_descriptions != 0 else 0
            )

            result["description_coverage"] = description_coverage
            result["test_coverage"] = result.get("has_column_test", False)
            result["missing_col_descr_count"] = len(missing_col_descr)
            result["missing_col_descr"] = missing_col_descr
            result["yml_missing_col_count"] = len(yml_missing_cols)
            result["yml_missing_cols"] = yml_missing_cols
            result["extra_yml_col_count"] = len(yml_extra_cols)
            result["extra_yml_cols"] = yml_extra_cols

        return results

    def total_coverage_stats(self, joined_models: list):

        total_models = 0
        models_w_tests = 0
        col_desc_coverages = []
        for model in joined_models:

            # gather total test coverage
            total_models += 1
            if model["test_coverage"] is True:
                models_w_tests += 1

            col_desc_coverages.append(model["description_coverage"])

        total_test_coverage = 100 * (models_w_tests / total_models)
        average_desc_coverage = sum(col_desc_coverages) / len(col_desc_coverages)
        print(f"Project Test Coverage: {self._format_percentage_local(total_test_coverage)}")
        print(f"Project Docs Coverage: {self._format_percentage_local(average_desc_coverage)}")

    def total_coverage_stats_tbl(self, joined_models: list, gh_comment: bool = False):
        if gh_comment:
            tbl_list = [
                (
                    f"{d['parent']}.{d['model_name']}",
                    self._format_percentage_gh(d["description_coverage"]),
                    self._format_boolean_gh(d["test_coverage"]),
                )
                for d in joined_models
            ]
        else:
            tbl_list = [
                (
                    f"{d['parent']}.{d['model_name']}",
                    self._format_percentage_local(d["description_coverage"]),
                    self._format_boolean_local(d["test_coverage"]),
                )
                for d in joined_models
            ]

        table = tabulate(
            tbl_list,
            headers=["Model", "Descr. Coverage", "Test Coverage"],
            tablefmt="github",
        )
        return table

    def column_report(self, joined_models: list):
        output = ""
        for d in joined_models:
            if d["extra_yml_col_count"] > 0 or d["yml_missing_col_count"] > 0 or d["missing_col_descr_count"] > 0:
                output += "\n\n---"
                output += f"{d['parent'].upper()}.{d['model_name'].upper()} COLUMN DISCREPANCIES"
                output += "---"

                if d["extra_yml_col_count"] > 0:
                    output += "\nThe following columns don't exist in SQL and can be removed from properties.yml:"
                for col in d.get("extra_yml_cols", []):
                    output += f"\n - {col}"

                if d["yml_missing_col_count"] > 0:
                    output += "\nThe following columns are completely missing in the properties.yml:"
                for col in d.get("yml_missing_cols", []):
                    output += f"\n - {col}"

                if d["missing_col_descr_count"] > 0:
                    output += "\nThe following columns are missing descriptions in properties.yml:"
                for col in d.get("missing_col_descr", []):
                    output += f"\n - {col}"

        return output

    def column_report_md(self, joined_models: list) -> str:
        """
        Generate the column report in GitHub-friendly Markdown format.
        :param joined_models: List of joined models with column discrepancies
        :return: GitHub-friendly Markdown formatted report
        """
        markdown_report = ""

        for d in joined_models:
            if d["extra_yml_col_count"] > 0 or d["yml_missing_col_count"] > 0 or d["missing_col_descr_count"] > 0:
                markdown_report += (
                    f"\n\n<details>\n\n <summary>{d['parent']}.{d['model_name']} column details</summary>\n\n"
                )

                if d["extra_yml_col_count"] > 0:
                    markdown_report += (
                        "\nThe following columns don't exist in SQL and can be removed from properties.yml:\n"
                    )
                    for col in d.get("extra_yml_cols", []):
                        markdown_report += f" - {col}\n"

                if d["yml_missing_col_count"] > 0:
                    markdown_report += "\nThe following columns are completely missing in the properties.yml:\n"
                    for col in d.get("yml_missing_cols", []):
                        markdown_report += f" - {col}\n"

                if d["missing_col_descr_count"] > 0:
                    markdown_report += "\nThe following columns are missing descriptions in properties.yml:\n"
                    for col in d.get("missing_col_descr", []):
                        markdown_report += f" - {col}\n"

                markdown_report += "\n\n</details>"

        return markdown_report

    def _colorize(self, value, color):
        # ANSI color codes for terminal output
        color_codes = {"green": "\033[92m", "red": "\033[91m", "reset": "\033[0m"}
        return f"{color_codes[color]}{value}{color_codes['reset']}"

    def _format_boolean_local(self, value):
        return self._colorize("✔" if value else "✘", "green" if value else "red")

    def _format_boolean_gh(self, value):
        return ":white_check_mark:" if value else ":x:"

    def _format_percentage_local(self, value, threshold: int = 80):
        if value < threshold:
            return self._colorize(f"{round(value, 2)}%", "red")
        else:
            return self._colorize(f"{round(value, 2)}%", "green")

    def _format_percentage_gh(self, value, threshold: int = 80):
        if value < threshold:
            return f":warning: {round(value, 2)}%"
        else:
            return f":medal_sports: {round(value, 2)}%"


if __name__ == "__main__":
    # Get the command line arguments directly
    args = sys.argv[1:]

    # Extract project_dir and filtered_models from args
    project_dir_index = args.index("--project_dir") if "--project_dir" in args else None
    filtered_models_index = args.index("--filtered_models") if "--filtered_models" in args else None

    # Get the values or set default values
    project_dir = args[project_dir_index + 1] if project_dir_index is not None else None
    filtered_models = args[filtered_models_index + 1].split() if filtered_models_index is not None else []

    # Create an instance of DbtCoverage with the provided arguments
    dbt_parse = DbtCoverage(project_dir=project_dir, filtered_models=filtered_models)

    # Get the joined models
    joined_models = dbt_parse.combine_yml_and_sql()

    # this infers the script is running in GitHub
    if os.getenv("GITHUB_OUTPUT", "UNAVAILABLE") != "UNAVAILABLE":
        # Append to GITHUB_OUTPUT
        stats_tbl = dbt_parse.total_coverage_stats_tbl(joined_models=joined_models, gh_comment=True)
        column_report = dbt_parse.column_report_md(joined_models=joined_models)
        report_output = f"{stats_tbl}\n{column_report}"
        set_multiline_output("report_output", report_output)

    # this is for local runs
    else:
        # Output total coverage stats table
        stats_tbl = dbt_parse.total_coverage_stats_tbl(joined_models=joined_models)
        column_report = dbt_parse.column_report(joined_models=joined_models)
        report_output = f"{stats_tbl}\n{column_report}"
        print(report_output)