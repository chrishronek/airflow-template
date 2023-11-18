from __future__ import annotations

from typing import Any

import logging
import os
import shutil

from cosmos.operators.local import DbtDocsLocalOperator, DbtLocalBaseOperator

logger = logging.getLogger(__name__)


class DbtFreshnessLocalOperator(DbtLocalBaseOperator):
    """
    Executes the `dbt source freshness` command
    Use the `callback` parameter to specify a callback function to run after the command completes.
    """

    ui_color = "#8194E0"

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.base_cmd = ["source", "freshness"]

class DbtFreshnessStupidOperaotr(DbtFreshnessLocalOperator):
    """
    Executes `dbt source freshness` command and saves to local storage.

    :param folder_dir: This can be used to specify under which directory the generated DBT documentation should be
        uploaded.
    """

    ui_color = "#FF9900"

    def __init__(
        self,
        **kwargs: str,
    ) -> None:
        "Initializes the operator."
        super().__init__(**kwargs)

        # override the callback with our own
        self.callback = self.save_to_dir

    def save_to_dir(self, project_dir: str) -> None:

        target_dir = f"{project_dir}/target"
        destination_dir = os.path.join(self.project_dir, "target")

        if not os.path.exists(destination_dir):
            os.makedirs(destination_dir)

        for filename in ["sources.json"]:
            source_file = os.path.join(target_dir, filename)
            destination_file = os.path.join(destination_dir, filename)
            shutil.copyfile(source_file, destination_file)

class DbtDocsStupidOperator(DbtDocsLocalOperator):
    """
    Executes `dbt docs generate` command and saves to local storage.

    :param folder_dir: This can be used to specify under which directory the generated DBT documentation should be
        uploaded.
    """

    ui_color = "#FF9900"

    def __init__(
        self,
        **kwargs: str,
    ) -> None:
        "Initializes the operator."
        super().__init__(**kwargs)

        # override the callback with our own
        self.callback = self.save_to_dir

    def save_to_dir(self, project_dir: str) -> None:

        target_dir = f"{project_dir}/target"
        destination_dir = os.path.join(self.project_dir, "target")

        if not os.path.exists(destination_dir):
            os.makedirs(destination_dir)


        for filename in ["manifest.json", "catalog.json", "run_results.json"]:
            source_file = os.path.join(target_dir, filename)
            destination_file = os.path.join(destination_dir, filename)
            shutil.copyfile(source_file, destination_file)
