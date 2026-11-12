"""The Airflow plugin. Airflow finds it through the ``airflow.plugins`` entry point."""

from __future__ import annotations

import logging
from typing import Any

try:
    from airflow.plugins_manager import AirflowPlugin  # type: ignore[import-not-found]
except ImportError:  # a Task SDK worker without Airflow core
    from airflow.sdk.plugins_manager import AirflowPlugin  # type: ignore[import-not-found,no-redef]

logger = logging.getLogger("slashpeak.airflow")


class SlashPeakPlugin(AirflowPlugin):  # type: ignore[misc]
    """Turns on the SlashPeak Job Router BigQuery hook when Airflow loads plugins."""

    name = "slashpeak"

    @classmethod
    def on_load(cls, *args: Any, **kwargs: Any) -> None:
        try:
            from slashpeak.airflow import _on_plugin_load

            _on_plugin_load()
        except Exception as error:  # noqa: BLE001
            logger.debug("slashpeak airflow plugin skipped", extra={"code": "load_error", "error": type(error).__name__})
