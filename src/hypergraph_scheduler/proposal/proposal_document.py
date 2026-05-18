from __future__ import annotations

from collections import defaultdict

from hypergraph_scheduler.scheduling.models import ProposalRow, RepresentativeRunProfile, RuntimeEstimationConfig, TaskSumEstimate, WorkingHours
from hypergraph_scheduler.scheduling.runtime_estimation import (
    choose_recommender_processing_seconds,
    choose_typical_runtime_seconds,
    profile_completion_minutes,
    profile_processing_minutes,
    profile_start_delay_minutes,
    proposal_effective_window_minutes,
)
from hypergraph_scheduler.scheduling.slot_optimization import average_global_pressure_for_window, task_load_weight
from hypergraph_scheduler.scheduling.time_utils import add_minutes, format_duration_minutes, format_minute_of_day, parse_cron_hours, parse_hhmm


def _format_seconds_as_hours(value: float | None) -> str:
    return f"{(value or 0.0) / 3600.0:.2f}"


def render_reviewed_assumptions_markdown(
    *,
    scope_display_name: str,
    solver_backend: str,
    solver_objective_mode: str,
    reviewed_assumption_rows: list[dict[str, object]],
) -> str:
    lines = [
        f"# {scope_display_name} Reviewed Assumptions",
        "",
        "This file is the review surface for the active scheduling inputs.",
        "Treat these rows as the assumptions to validate before debating the proposed schedule.",
        f"Active backend: `{solver_backend}`.",
        f"Active objective mode: `{solver_objective_mode}`.",
        "",
        "Confidence guide:",
        "- `hard_fact`: structural input or fixed schedule that should not be debated through historical heuristics.",
        "- `reviewed_assumption`: manually reviewed runtime or operational rule that is intentionally trusted over noisy history.",
        "- `advisory_history`: history-derived value kept for context but not trusted as strongly.",
        "",
        "| DAG | Current schedule | Movability | Runtime source | Reviewed runtime | Upstream ready UTC | Upstream-ready source | Dependency gate | Post-ready setup | Confidence | Notes |",
        "| --- | --- | --- | --- | ---: | --- | --- | ---: | ---: | --- | --- |",
    ]
    for row in reviewed_assumption_rows:
        lines.append(
            "| {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} |".format(
                row["dag_id"],
                row["current_schedule"],
                row["movability"],
                row["runtime_source"],
                row["reviewed_runtime"],
                row["upstream_ready_utc"],
                row["upstream_ready_source"],
                row["dependency_gate"],
                row["post_ready_setup"],
                row["confidence"],
                row["notes"] or "-",
            )
        )
    return "\n".join(lines) + "\n"


def render_why_each_dag_moved_markdown(
    *,
    scope_display_name: str,
    solver_backend: str,
    solver_objective_mode: str,
    proposal_rows: list[ProposalRow],
    reviewed_assumption_rows: list[dict[str, object]],
) -> str:
    reviewed_assumptions_by_dag = {
        str(row["dag_id"]): row for row in reviewed_assumption_rows if row.get("dag_id")
    }
    lines = [
        f"# {scope_display_name} Why Each DAG Moved",
        "",
        "This file explains the proposed move for each DAG in plain operational terms.",
        f"Active backend: `{solver_backend}`.",
        f"Active objective mode: `{solver_objective_mode}`.",
        "",
        "| DAG | Current schedule | Proposed schedule | Shift | Waiting saved | Why this moved |",
        "| --- | --- | --- | ---: | ---: | --- |",
    ]

    for proposal in proposal_rows:
        reviewed_assumption_row = reviewed_assumptions_by_dag.get(proposal.dag_id, {})
        dependency_gate = str(reviewed_assumption_row.get("dependency_gate", "0m"))
        movability = str(reviewed_assumption_row.get("movability", ""))
        notes = str(reviewed_assumption_row.get("notes", "")).strip()
        rationale_parts: list[str] = []

        if proposal.shift_minutes == 0:
            if movability == "fixed_multi_slot":
                rationale_parts.append("kept in place because this is a fixed multi-slot schedule")
            elif proposal.wait_saved_minutes <= 0:
                rationale_parts.append("kept in place because the reviewed-ready timing did not justify a safer alternative slot")
            else:
                rationale_parts.append("kept in place after applying the reviewed timing constraints")
        else:
            direction = "later" if proposal.shift_minutes > 0 else "earlier"
            rationale_parts.append(
                f"moved {format_duration_minutes(abs(proposal.shift_minutes))} {direction}"
            )
            if proposal.wait_saved_minutes > 0:
                rationale_parts.append(
                    f"to remove {format_duration_minutes(proposal.wait_saved_minutes)} of pre-ready waiting"
                )
            elif proposal.proposed_gap_after_ready_minutes > proposal.current_gap_after_ready_minutes:
                rationale_parts.append("to start after the reviewed upstream-ready time instead of before it")
            else:
                rationale_parts.append("to reduce modeled concurrency pressure while preserving feasibility")

        if dependency_gate != "0m":
            rationale_parts.append(f"while honoring a {dependency_gate} dependency gate")
        if proposal.post_ready_setup_minutes > 0:
            rationale_parts.append(
                f"with {format_duration_minutes(proposal.post_ready_setup_minutes)} of modeled post-ready setup"
            )
        if notes and notes != "-":
            rationale_parts.append(notes.replace(" | ", "; "))

        lines.append(
            "| {} | {} | {} | {} | {} | {} |".format(
                proposal.dag_id,
                proposal.current_schedule,
                proposal.proposed_schedule,
                format_duration_minutes(abs(proposal.shift_minutes)),
                format_duration_minutes(proposal.wait_saved_minutes),
                "; ".join(rationale_parts),
            )
        )

    return "\n".join(lines) + "\n"


def render_schedule_proposal_markdown(
    *,
    scope_display_name: str,
    scope_id: str,
    solver_backend: str,
    solver_objective_mode: str,
    sequential_dag_pairs: tuple[tuple[str, str], ...],
    solver_status: str,
    solver_rejection_reason: str | None,
    proposal_rows: list[ProposalRow],
    working_hours: WorkingHours,
    bucket_minutes: int,
    min_gap_minutes: int,
    rescheduled_count: int,
    total_wait_saved_minutes: int,
    reviewed_assumptions_csv_name: str,
    reviewed_assumptions_markdown_name: str,
    why_each_dag_moved_markdown_name: str,
    reviewed_assumption_rows: list[dict[str, object]],
    include_runtime_diagnostics: bool,
    observed_global_limits_csv_name: str,
    observed_per_dag_limits_csv_name: str,
    representative_profiles: dict[str, RepresentativeRunProfile | None],
    runtime_estimation_config: RuntimeEstimationConfig,
    diagnostics_by_dag: dict[str, list[tuple[object, ...]]],
    task_sum_estimates: dict[str, TaskSumEstimate],
    task_count_estimates: dict[str, float],
    global_pressure_by_minute: dict[int, float],
    current_global_pressure_hourly: list[float],
    proposed_global_pressure_hourly: list[float],
    current_ds_pressure_hourly: list[float],
    proposed_ds_pressure_hourly: list[float],
    hourly_pressure_csv_name: str,
    mermaid_chart_name: str,
    global_mermaid_chart_name: str,
    append_hourly_table: callable,
    append_hourly_delta_summary: callable,
) -> str:
    solver_descriptions = {
        "greedy": "a greedy heuristic backend",
        "cp_sat": "the CP-SAT global optimizer backend",
        "milp": "the MILP comparison backend",
    }
    lines = [
        f"# {scope_display_name} Schedule Proposal",
        "",
        f"This proposal uses {solver_descriptions.get(solver_backend, solver_backend)} for the DS-owned {scope_id} DAGs in this scope.",
        "It uses the existing working-hours constraint, the static dependency graph, and observed wait pressure from the DuckDB runtime views.",
        f"The active solver objective mode is `{solver_objective_mode}`.",
        "",
        f"Across {rescheduled_count} rescheduled DAGs, the proposal removes about {format_duration_minutes(total_wait_saved_minutes)} of pre-ready waiting time.",
        "",
        "## Reviewed Assumptions",
        "",
        "The Monday proposal should be read as a reviewed-assumptions schedule, not as a precise forecast from noisy history.",
        f"A machine-readable summary of the active runtime and dependency assumptions is written to `{reviewed_assumptions_csv_name}`.",
        f"A presentation-ready review of those same assumptions is written to `{reviewed_assumptions_markdown_name}`.",
        f"A per-DAG explanation of each proposed move is written to `{why_each_dag_moved_markdown_name}`.",
        "",
        "| DAG | Runtime source | Reviewed runtime | Upstream-ready source | Dependency gate | Confidence |",
        "| --- | --- | ---: | --- | ---: | --- |",
    ]
    for reviewed_assumption_row in reviewed_assumption_rows:
        lines.append(
            "| {} | {} | {} | {} | {} | {} |".format(
                reviewed_assumption_row["dag_id"],
                reviewed_assumption_row["runtime_source"],
                reviewed_assumption_row["reviewed_runtime"],
                reviewed_assumption_row["upstream_ready_source"],
                reviewed_assumption_row["dependency_gate"],
                reviewed_assumption_row["confidence"],
            )
        )

    lines.extend(
        [
            "",
        "Scheduling rules:",
        f"- working-hours window: {format_minute_of_day(working_hours.earliest_start_minute)}-{format_minute_of_day(working_hours.latest_start_minute)} UTC",
        f"- time bucket size: {bucket_minutes} minutes",
        f"- minimum stagger gap: {min_gap_minutes} minutes",
        "- reschedulable DAGs are treated as effectively starting when create_config or create_run_config begins",
        "- finish by 19:00 UTC is modeled as a strong soft penalty using a robust DAG runtime estimate from dag_run start/end timestamps",
        "- markdown graph uses the modeled proposal timing shown in the table",
        "- csv rows keep the aggregate optimizer inputs used for scoring the schedule proposal",
        "- effective start is estimated as max(proposed cron, estimated upstream-ready time) plus a small observed post-ready setup lag",
        "- estimated upstream-ready time uses the maximum mapped clipped-median ready time across direct seed edges",
        "- per-run upstream-ready samples are clipped at 20 hours before the edge medians are computed",
        "- waiting before upstream readiness is penalized more heavily than starting shortly after readiness",
        "- dependency pressure buffer derived from mapped edge P90 idle wait",
        "- multi-slot schedules are currently kept unchanged",
        f"- active scheduling backend: {solver_backend}",
        f"- active objective mode: {solver_objective_mode}",
        "",
        ]
    )
    if include_runtime_diagnostics:
        lines.insert(lines.index("- csv rows keep the aggregate optimizer inputs used for scoring the schedule proposal"), "- representative successful runs are kept only as historical diagnostics")
        lines.insert(lines.index("- csv rows keep the aggregate optimizer inputs used for scoring the schedule proposal"), "- schedule-details tables show modeled effective finishes used by the solver and representative finishes as reference columns")
    if sequential_dag_pairs:
        pair_text = ", ".join(f"{primary} -> {secondary}" for primary, secondary in sequential_dag_pairs)
        lines.append(f"- explicit sequential DAG pairs: {pair_text}")
        lines.append("- explicit sequential pairs are enforced on modeled effective starts and modeled effective finishes, not on representative finish columns")
    lines.extend(
        [
            "## Waiting Saved",
            "",
            "| DAG | Current wait before ready | Proposed wait before ready | Waiting saved | Estimated upstream ready UTC |",
            "| --- | ---: | ---: | ---: | --- |",
        ]
    )
    if solver_status == "rejected":
        lines[lines.index("## Waiting Saved"):lines.index("## Waiting Saved")] = [
            "## Solver Outcome",
            "",
            "No acceptable alternative schedule was found under the current hard concurrency-first rule.",
            f"The solver rejection reason is `{solver_rejection_reason or 'unknown'}`.",
            "The proposal rows below therefore keep the current schedules unchanged for the affected DAGs.",
            "",
        ]

    for proposal in proposal_rows:
        lines.append(
            "| {} | {} | {} | {} | {} |".format(
                proposal.dag_id,
                format_duration_minutes(proposal.current_wait_before_ready_minutes),
                format_duration_minutes(proposal.proposed_wait_before_ready_minutes),
                format_duration_minutes(proposal.wait_saved_minutes),
                proposal.estimated_upstream_ready_utc,
            )
        )

    lines.extend(
        [
            "",
            "## Observed Airflow Limit Comparison",
            "",
            "Exact observed overlap peaks from task-instance and dag-run intervals are exported for direct comparison against current Airflow concurrency settings.",
            f"Observed global and scoped limits are written to `{observed_global_limits_csv_name}`.",
            f"Observed per-DAG task and active-run limits are written to `{observed_per_dag_limits_csv_name}`.",
            "",
            "## Global Pressure Diagnostics",
            "",
            "Median global running-task counts across all DAGs, averaged over each DAG's estimated run window.",
            "",
            "| DAG | Task load weight | Current global pressure | Proposed global pressure |",
            "| --- | ---: | ---: | ---: |",
        ]
    )
    for proposal in proposal_rows:
        profile = representative_profiles.get(proposal.dag_id)
        current_effective_minutes, proposed_effective_minutes, processing_minutes = proposal_effective_window_minutes(
            proposal,
            profile,
            runtime_estimation_config,
        )
        current_pressure = average_global_pressure_for_window(
            global_pressure_by_minute,
            current_effective_minutes,
            current_effective_minutes + processing_minutes,
            bucket_minutes,
        )
        proposed_pressure = average_global_pressure_for_window(
            global_pressure_by_minute,
            proposed_effective_minutes,
            proposed_effective_minutes + processing_minutes,
            bucket_minutes,
        )
        lines.append(
            "| {} | {:.2f} | {:.2f} | {:.2f} |".format(
                proposal.dag_id,
                task_load_weight(task_count_estimates.get(proposal.dag_id)),
                current_pressure,
                proposed_pressure,
            )
        )

    lines.extend(
        [
            "",
            "## Pressure Evolution by UTC Hour",
            "",
            "Hourly averages derived from the 15-minute median global running-task profile.",
            "DS pressure remains an average concurrent-task estimate derived from task-sum divided by runtime.",
            "Current peak parallel-task columns now use exact observed bucket maxima from historical task overlaps.",
            "Proposed peak parallel-task columns remain schedule-model estimates, because future overlaps are not directly observed yet.",
            "The proposed pressure view is an estimate: historical global pressure minus the current DS-scoped parallel-task load plus the proposed DS-scoped parallel-task load.",
            f"The hourly CSV used for downstream notebook analysis is also written to `{hourly_pressure_csv_name}`.",
            f"A Mermaid live-renderable hourly global-load chart across all DAGs is also written to `{mermaid_chart_name}`.",
            f"A global-only Mermaid chart is also written to `{global_mermaid_chart_name}`.",
            "",
        ]
    )
    append_hourly_table(
        lines,
        "Estimated global pressure",
        "Before proposal",
        current_global_pressure_hourly,
        "After proposal",
        proposed_global_pressure_hourly,
    )
    append_hourly_delta_summary(lines, current_global_pressure_hourly, proposed_global_pressure_hourly)
    lines.append("")
    append_hourly_table(
        lines,
        "Estimated DS parallel tasks",
        "Before proposal",
        current_ds_pressure_hourly,
        "After proposal",
        proposed_ds_pressure_hourly,
    )
    append_hourly_delta_summary(lines, current_ds_pressure_hourly, proposed_ds_pressure_hourly)

    lines.extend(
        [
            "",
            "## Chronological Graph",
            "",
            "```mermaid",
            "gantt",
            f"    title {scope_display_name} Current vs Proposed Timing",
            "    dateFormat HH:mm",
            "    axisFormat %H:%M",
        ]
    )

    for proposal in proposal_rows:
        profile = representative_profiles.get(proposal.dag_id)
        current_start = proposal.current_primary_start_utc
        proposed_start = proposal.proposed_primary_start_utc
        ready_time = proposal.estimated_upstream_ready_utc
        current_effective_minutes, proposed_effective_minutes, processing_minutes = proposal_effective_window_minutes(
            proposal,
            profile,
            runtime_estimation_config,
        )
        current_effective = format_minute_of_day(current_effective_minutes)
        proposed_effective = format_minute_of_day(proposed_effective_minutes)
        current_total_prerun_delay_minutes = max(0, current_effective_minutes - parse_hhmm(current_start))
        proposed_total_prerun_delay_minutes = max(0, proposed_effective_minutes - parse_hhmm(proposed_start))
        wait_saved = proposal.wait_saved_minutes

        current_lines = [
            f"    section {proposal.dag_id} current",
            f"    Waiting time ({format_duration_minutes(current_total_prerun_delay_minutes)}) :crit, {current_start}, {max(current_total_prerun_delay_minutes, 1)}m",
            f"    Current run ({format_duration_minutes(processing_minutes)}) :active, {current_effective}, {max(processing_minutes, 1)}m",
        ]

        proposed_lines = [
            f"    section {proposal.dag_id} proposed",
            f"    Waiting time ({format_duration_minutes(proposed_total_prerun_delay_minutes)}) :active, {proposed_start}, {max(proposed_total_prerun_delay_minutes, 1)}m",
            f"    Proposed run ({format_duration_minutes(processing_minutes)}) :active, {proposed_effective}, {max(processing_minutes, 1)}m",
            f"    Wait saved ({format_duration_minutes(wait_saved)}) :done, {proposed_start}, {max(wait_saved, 1)}m",
        ]
        proposed_lines.insert(2, f"    Upstream ready ({ready_time}) :done, {ready_time}, 1m")

        lines.extend(current_lines + proposed_lines)

    lines.extend(["    section utc scale", "    00:00 UTC anchor :done, 00:00, 1m"])
    lines.extend(
        [
            "```",
            "",
            "## Schedule Details",
            "",
            (
                "| DAG | Current schedule | Proposed schedule | Current cron start UTC | Proposed cron start UTC | Recent observed effective start UTC | Estimated upstream ready UTC | Proposed effective start UTC | Proposed gap after ready | Modeled current effective finish UTC | Modeled proposed effective finish UTC | Representative current finish UTC | Representative proposed finish UTC | Waiting saved | Shift min | Post-ready setup min | Pressure buffer min | Strategy |"
                if include_runtime_diagnostics
                else "| DAG | Current schedule | Proposed schedule | Current cron start UTC | Proposed cron start UTC | Recent observed effective start UTC | Estimated upstream ready UTC | Proposed effective start UTC | Proposed gap after ready | Modeled current effective finish UTC | Modeled proposed effective finish UTC | Waiting saved | Shift min | Post-ready setup min | Pressure buffer min | Strategy |"
            ),
            (
                "| --- | --- | --- | --- | --- | --- | --- | --- | ---: | --- | --- | --- | --- | ---: | ---: | ---: | ---: | --- |"
                if include_runtime_diagnostics
                else "| --- | --- | --- | --- | --- | --- | --- | --- | ---: | --- | --- | ---: | ---: | ---: | ---: | --- |"
            ),
        ]
    )

    for proposal in proposal_rows:
        profile = representative_profiles.get(proposal.dag_id)
        modeled_current_effective_minutes, modeled_proposed_effective_minutes, modeled_processing_minutes = proposal_effective_window_minutes(
            proposal,
            profile,
            runtime_estimation_config,
        )
        modeled_current_finish = format_minute_of_day(modeled_current_effective_minutes + modeled_processing_minutes)
        modeled_proposed_finish = format_minute_of_day(modeled_proposed_effective_minutes + modeled_processing_minutes)
        row_values = [
            proposal.dag_id,
            proposal.current_schedule,
            proposal.proposed_schedule,
            proposal.current_primary_start_utc,
            proposal.proposed_primary_start_utc,
            proposal.recent_observed_effective_start_utc,
            proposal.estimated_upstream_ready_utc,
            proposal.proposed_effective_start_utc,
            format_duration_minutes(proposal.proposed_gap_after_ready_minutes),
            modeled_current_finish,
            modeled_proposed_finish,
            str(proposal.wait_saved_minutes),
            str(proposal.shift_minutes),
            str(proposal.post_ready_setup_minutes),
            str(proposal.pressure_buffer_minutes),
            proposal.strategy,
        ]
        if include_runtime_diagnostics:
            representative_start_delay_minutes = profile_start_delay_minutes(profile, proposal.effective_start_delay_minutes)
            representative_processing_minutes = profile_processing_minutes(
                profile,
                int(round(choose_typical_runtime_seconds(proposal.avg_dag_runtime_seconds, proposal.median_dag_runtime_seconds) / 60.0)),
            )
            representative_completion_minutes = profile_completion_minutes(
                profile,
                representative_start_delay_minutes + representative_processing_minutes,
            )
            display_current_finish = format_minute_of_day(
                add_minutes(parse_hhmm(proposal.current_primary_start_utc), representative_completion_minutes)
            )
            display_proposed_finish = format_minute_of_day(
                add_minutes(parse_hhmm(proposal.proposed_primary_start_utc), representative_completion_minutes)
            )
            row_values[10:10] = [display_current_finish, display_proposed_finish]
        lines.append("| " + " | ".join(row_values) + " |")

    if include_runtime_diagnostics and diagnostics_by_dag:
        lines.extend(
            [
                "",
                "## Upstream Ready Diagnostics",
                "",
                "These are the per-run mapped upstream-ready samples used to diagnose outliers behind the proposal.",
                "Raw ready seconds come from sensor touch plus idle wait; clipped ready seconds are capped at 20 hours before median aggregation.",
            ]
        )

        for proposal in proposal_rows:
            dag_diagnostics = diagnostics_by_dag.get(proposal.dag_id)
            if not dag_diagnostics:
                continue
            lines.extend(
                [
                    "",
                    f"### {proposal.dag_id}",
                    "",
                    "| Upstream DAG | Sensor task | Run ID | Logical date | Raw ready h after schedule | Clipped ready h after schedule | Clipped |",
                    "| --- | --- | --- | --- | ---: | ---: | --- |",
                ]
            )
            for diagnostic_row in dag_diagnostics:
                lines.append(
                    "| {} | {} | {} | {} | {} | {} | {} |".format(
                        diagnostic_row[1],
                        diagnostic_row[2],
                        diagnostic_row[3],
                        diagnostic_row[4],
                        _format_seconds_as_hours(float(diagnostic_row[5]) if diagnostic_row[5] is not None else 0.0),
                        _format_seconds_as_hours(float(diagnostic_row[6]) if diagnostic_row[6] is not None else 0.0),
                        "yes" if diagnostic_row[7] else "no",
                    )
                )

    if include_runtime_diagnostics:
        lines.extend(
            [
                "",
                "## Runtime Estimation Comparison",
                "",
                "These estimates are shown side-by-side so noisy metadata does not force a single runtime interpretation.",
                "When a reviewed manual runtime override exists, the scheduler uses it directly; otherwise it falls back to the historical processing summary available for that DAG.",
                "Representative runs and task-sum estimates remain comparison aids and diagnostic context, not solver truth for Monday by themselves.",
                "",
                "| DAG | Aggregate create_config/create_run_config h | Representative run h | Task-sum filtered h | Manual override h |",
                "| --- | ---: | ---: | ---: | ---: |",
            ]
        )
        for proposal in proposal_rows:
            profile = representative_profiles.get(proposal.dag_id)
            task_sum_estimate = task_sum_estimates.get(proposal.dag_id)
            manual_override_seconds = runtime_estimation_config.manual_overrides_seconds.get(proposal.dag_id)
            lines.append(
                "| {} | {} | {} | {} | {} |".format(
                    proposal.dag_id,
                    _format_seconds_as_hours(proposal.median_effective_processing_seconds),
                    _format_seconds_as_hours(profile.processing_seconds) if profile is not None else "-",
                    _format_seconds_as_hours(task_sum_estimate.median_task_sum_seconds) if task_sum_estimate is not None else "-",
                    _format_seconds_as_hours(manual_override_seconds) if manual_override_seconds is not None else "-",
                )
            )

        non_null_profiles = {dag_id: profile for dag_id, profile in representative_profiles.items() if profile is not None}
        if non_null_profiles:
            lines.extend(
                [
                    "",
                    "## Representative Successful Runs",
                    "",
                    "These rows review one normal-looking successful run per DAG, chosen from filtered non-outlier metadata.",
                    "When a `create_config` or `create_run_config` anchor exists, the profile uses that task start -> end; otherwise it falls back to the Airflow dag run runtime.",
                    "",
                    "| DAG | Chosen run | Logical date | Anchor | Start delay h | Work h | Schedule-to-end h | Finish at current cron UTC |",
                    "| --- | --- | --- | --- | ---: | ---: | ---: | --- |",
                ]
            )
            for proposal in proposal_rows:
                profile = non_null_profiles.get(proposal.dag_id)
                if profile is None:
                    continue
                minute, hours, _ = parse_cron_hours(proposal.current_schedule)
                current_finish_utc = format_minute_of_day(
                    add_minutes(min(hours) * 60 + minute, int(round(profile.schedule_to_end_seconds / 60.0)))
                )
                lines.append(
                    "| {} | {} | {} | {} | {} | {} | {} | {} |".format(
                        proposal.dag_id,
                        profile.run_id,
                        profile.logical_date,
                        profile.anchor,
                        _format_seconds_as_hours(profile.start_delay_seconds),
                        _format_seconds_as_hours(profile.processing_seconds),
                        _format_seconds_as_hours(profile.schedule_to_end_seconds),
                        current_finish_utc,
                    )
                )

    return "\n".join(lines) + "\n"