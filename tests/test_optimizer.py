from __future__ import annotations

import csv
import json

from hypergraph_scheduler.optimizer import (
    MODEL_PATH,
    WorkingHours,
    build_recommendation_engine_schedule_proposal,
    choose_primary_start_slot,
    finalize_output_row,
    format_duration_minutes,
    parse_cron_hours,
)


def test_choose_primary_start_slot_prefers_upstream_ready_window() -> None:
    slot, effective = choose_primary_start_slot(
        current_primary_start_minute=7 * 60 + 5,
        assigned_effective_starts=[],
        working_hours=WorkingHours(earliest_start_minute=8 * 60, latest_start_minute=18 * 60),
        bucket_minutes=15,
        min_gap_minutes=45,
        finish_deadline_minute=19 * 60,
        effective_processing_minutes=443,
        upstream_ready_minute=10 * 60 + 30,
        post_ready_setup_minutes=9,
    )

    assert slot == 10 * 60 + 30
    assert effective == 10 * 60 + 39


def test_choose_primary_start_slot_prefers_late_slot_over_gap_violation() -> None:
    slot, effective = choose_primary_start_slot(
        current_primary_start_minute=9 * 60,
        assigned_effective_starts=[9 * 60 + 5],
        working_hours=WorkingHours(earliest_start_minute=8 * 60, latest_start_minute=10 * 60),
        bucket_minutes=15,
        min_gap_minutes=45,
        finish_deadline_minute=19 * 60,
        effective_processing_minutes=30,
        upstream_ready_minute=9 * 60,
        post_ready_setup_minutes=0,
    )

    assert slot == 10 * 60
    assert effective == slot


def test_finalize_output_row_removes_internal_keys() -> None:
    result = finalize_output_row(
        {
            "dag_id": "recipe_recommender",
            "strategy": "upstream_ready_slot_search",
            "current_primary_start_minute": 425,
            "current_effective_start_minute": 639,
            "effective_start_delay_minutes_raw": 214,
            "effective_processing_minutes_raw": 443,
            "typical_processing_minutes_raw": 443,
            "schedule_suffix": "* * 3",
        }
    )

    assert result == {
        "dag_id": "recipe_recommender",
        "strategy": "upstream_ready_slot_search",
    }


def test_parse_cron_hours_and_format_duration_minutes() -> None:
    minute, hours, suffix = parse_cron_hours("30 4,18 * * *")

    assert minute == 30
    assert hours == [4, 18]
    assert suffix == "* * *"
    assert format_duration_minutes(205) == "3h 25m"
    assert format_duration_minutes(60) == "1h"
    assert format_duration_minutes(5) == "5m"


class _FakeExecuteResult:
    def __init__(self, rows: list[tuple[object, ...]]) -> None:
        self._rows = rows

    def fetchall(self) -> list[tuple[object, ...]]:
        return self._rows


class _FakeConnection:
    def __init__(self, rows: list[tuple[object, ...]]) -> None:
        self.rows = rows
        self.queries: list[str] = []

    def execute(self, query: str) -> _FakeExecuteResult:
        self.queries.append(query)
        return _FakeExecuteResult(self.rows)


def test_build_schedule_proposal_writes_markdown_and_csv(monkeypatch, tmp_path) -> None:
    model_path = tmp_path / "recommendation_engine_schedule_optimization_model.json"
    model_path.write_text(
        json.dumps(
            {
                "optimization_defaults": {
                    "working_hours_constraint": {
                        "earliest_start": "08:00",
                        "latest_start": "18:00",
                    }
                }
            }
        ),
        encoding="utf-8",
    )

    rows = [
        (
            "recipe_recommender",
            "05 07 * * 3",
            3,
            30055.2,
            39863.4,
            214 * 60,
            250 * 60,
            443 * 60,
            443 * 60,
            500 * 60,
            1000.0,
            1000.0,
            60.0,
            205 * 60,
            220 * 60,
            0.0,
            0.0,
        ),
        (
            "menu_ranker",
            "30 4,18 * * *",
            0,
            805.4,
            1217.0,
            2 * 60,
            3 * 60,
            13 * 60,
            13 * 60,
            14 * 60,
            0.0,
            0.0,
            0.0,
            2 * 60,
            2 * 60,
            0.0,
            0.0,
        ),
    ]
    connection = _FakeConnection(rows)

    monkeypatch.setattr("hypergraph_scheduler.optimizer.MODEL_PATH", model_path)
    monkeypatch.setattr("hypergraph_scheduler.optimizer.ARTIFACTS_DIR", tmp_path)

    markdown_path = build_recommendation_engine_schedule_proposal(connection)

    csv_path = tmp_path / "recommendation_engine_schedule_proposal.csv"
    assert markdown_path == tmp_path / "recommendation_engine_schedule_proposal.md"
    assert markdown_path.exists()
    assert csv_path.exists()
    assert connection.queries

    markdown_text = markdown_path.read_text(encoding="utf-8")
    assert "# Recommendation Engine Schedule Proposal" in markdown_text
    assert "recipe_recommender" in markdown_text
    assert "30 10 * * 3" in markdown_text
    assert "kept_existing_multi_slot_schedule" in markdown_text

    with csv_path.open(newline="", encoding="utf-8") as csv_file:
        csv_rows = list(csv.DictReader(csv_file))

    assert [row["dag_id"] for row in csv_rows] == ["recipe_recommender", "menu_ranker"]
    assert csv_rows[0]["proposed_schedule"] == "30 10 * * 3"
    assert csv_rows[1]["strategy"] == "kept_existing_multi_slot_schedule"