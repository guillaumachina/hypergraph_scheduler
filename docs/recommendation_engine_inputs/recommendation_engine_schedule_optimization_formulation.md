# Recommendation Engine DAG Rescheduling Formulation

This document formulates the recommendation_engine DAG rescheduling problem as a constrained optimization problem built on the dependency graph in `recommendation_engine_dag_dependencies.json` and the scheduling-oriented model in `recommendation_engine_schedule_optimization_model.json`.

## Goal

The main operational goal is to reschedule the recommendation_engine DAGs so that:

1. they do not all reach long-wait upstream dependencies at the same time,
2. their execution is spread across DS team working hours,
3. all inter-DAG precedence constraints remain valid,
4. changes from the current schedule are controlled rather than arbitrary.

For now, the working-hours rule applies only to the recommendation_engine DAGs:

- `market_item_recommender`
- `recipe_recommender`
- `user_clustering_predict`
- `relevance_scoring`
- `menu_ranker`

## Sets And Indices

Let:

- $V$ be the set of DAGs included in the model.
- $R \subset V$ be the set of recommendation_engine DAGs to be actively rescheduled.
- $E \subset V \times V$ be the set of directed precedence edges.
- $K_i$ be the set of runs of DAG $i$ over a planning horizon.
- $T$ be the planning time horizon, in minutes or in discrete time buckets.

An edge $(i,j) \in E$ means DAG $j$ cannot begin its relevant run before DAG $i$ finishes the aligned upstream run.

## Parameters

For each DAG $i$ and run $k \in K_i$:

- $s^0_{ik}$: current scheduled start time.
- $d_{ik}$: expected runtime duration.
- $h_i \in \{0,1\}$: whether the DAG is subject to DS working-hours constraints.
- $[a,b]$: DS team working-hours window, common to all DAGs in $R$.

For each edge $(i,j) \in E$:

- $\delta_{ij}$: deterministic alignment offset when the dependency uses `execution_delta`.
- $A_{ij}(k)$: upstream run of $i$ aligned to downstream run $k$ of $j$.
- $c_{ij}$: optional safety buffer between upstream completion and downstream start.

For conditional edges:

- $q_{ij} \in \{0,1\}$ indicates whether the edge is enforced in the target environment. In the current production model, production-only hummus edges use $q_{ij}=1$.

## Decision Variables

For each recommendation_engine DAG $i \in R$ and run $k$:

- $x_{ik}$: optimized planned start time.

Derived completion time:

$$
f_{ik} = x_{ik} + d_{ik}
$$

Deviation from current schedule:

$$
\Delta_{ik} = x_{ik} - s^0_{ik}
$$

Optional waiting slack for dependency $(u,i)$:

$$
w_{uik} \ge 0
$$

with

$$
w_{uik} = x_{ik} - \left(f_{u,A_{ui}(k)} + c_{ui}\right)
$$

when the edge is active.

This $w_{uik}$ measures how long the downstream DAG is still delayed beyond the earliest feasible start after its upstream dependency completes.

## Core Constraints

### 1. Precedence Constraints

For every active edge $(u,i) \in E$ and run $k$:

$$
x_{ik} \ge f_{u,A_{ui}(k)} + c_{ui} \quad \text{if } q_{ui}=1
$$

Equivalently:

$$
x_{ik} \ge x_{u,A_{ui}(k)} + d_{u,A_{ui}(k)} + c_{ui}
$$

This is the hard feasibility condition.

### 2. Working-Hours Constraints

For all $i \in R$ and runs $k$:

$$
a \le x_{ik} \le b
$$

This working-hours window is global for the DS team and is not DAG-specific in the current model.

### 3. Optional Schedule Stability Bounds

If you do not want large shifts from the current cron-derived schedule, add:

$$
L_i \le \Delta_{ik} \le U_i
$$

where $L_i$ and $U_i$ are allowed backward/forward schedule shifts.

### 4. Optional Ordering Constraints Between Seed DAGs

If you want the recommendation_engine DAGs to be spread through working hours in a stable order, impose for chosen pairs $(i,j)$:

$$
x_{jk} - x_{ik} \ge g_{ij}
$$

where $g_{ij}$ is a minimum gap.

This is not required for feasibility, but it is useful if you want deliberate staggering.

## Objective Design

The objective should reflect your real priority: avoid synchronized waiting on long upstream chains, and spread seed DAG starts through working hours.

### Recommended Primary Objective

Minimize a weighted sum:

$$
\min \;
\alpha \sum_{i \in R} \sum_{k \in K_i} \sum_{u:(u,i)\in E} w_{uik}
+ \beta \sum_{t \in T} z_t
+ \gamma \sum_{i \in R} \sum_{k \in K_i} |\Delta_{ik}|
$$

where:

- the first term reduces downstream waiting after upstream completion,
- the second term penalizes start-time crowding,
- the third term penalizes large deviations from the current schedule.

Here $z_t$ is a concurrency or crowding penalty at time bucket $t$.

### Modeling Crowding

One practical definition is:

$$
n_t = \sum_{i \in R} \sum_{k \in K_i} \mathbf{1}[x_{ik} \in t]
$$

and then penalize the excess above a preferred level $M$:

$$
z_t \ge n_t - M, \quad z_t \ge 0
$$

This directly models the idea of avoiding many DAGs being released into long upstream waits at the same time.

## Why This Matches The Real Pain Point

The operational issue is not only that a DAG has upstream dependencies. The bigger issue is that several recommendation_engine DAGs are scheduled close together, then all sit idle behind the same or related upstream chains.

So the scheduling problem is closer to:

- do not release too many downstream DAGs into the same upstream bottleneck window,
- let upstream chains finish first when useful,
- then stagger downstream starts through the DS team’s working hours.

That is why minimizing pure schedule deviation is not enough. A better model explicitly penalizes overlapping downstream release into the same dependency bottleneck.

## Tradeoffs

Any optimization will trade off at least three competing goals.

### 1. Minimize Waiting Vs Preserve Current Cron Times

- If you prioritize low waiting, you may move DAGs substantially away from their current cron times.
- If you prioritize cron stability, you may preserve inefficient clustered waiting.

This is the tradeoff controlled by $\alpha$ versus $\gamma$.

### 2. Spread Starts Across Working Hours Vs Finish Earlier In The Day

- If you spread starts aggressively, some DAGs will start later even if they could have started earlier.
- If you start everything as early as feasible, you recreate concurrency spikes and shared waiting bottlenecks.

This is the tradeoff between crowding penalties and earliest-feasible-start behavior.

### 3. Enforce Business-Hours Windows Vs Respect Upstream Natural Availability

- If upstream data is naturally ready before working hours, forcing starts into working hours may create intentional idle time.
- If you relax working-hours constraints, you may reduce latency but lose the operational benefit of having runs happen when the DS team is active.

This is a deliberate business tradeoff, not a modeling flaw.

## Suggested Optimization Approaches

### Option 1. MILP With Time Buckets

Use discrete time buckets, binary start variables, and linear crowding penalties.

Pros:

- transparent objective and constraints,
- easy to explain to stakeholders,
- good if the planning horizon is not too large.

Cons:

- can grow quickly with many runs and time buckets,
- crowding terms may require extra binary variables.

This is a good first formal optimizer for the current problem.

### Option 2. CP-SAT / Constraint Programming

Model start times directly with interval-like or precedence constraints, and use penalty terms for crowding and schedule drift.

Pros:

- often easier for precedence-heavy scheduling,
- flexible for discrete logic and conditional constraints,
- good candidate when you add more operational rules later.

Cons:

- objective calibration can be less intuitive than MILP,
- explanation to non-OR users can be slightly harder.

This is likely the strongest practical option once the model becomes richer.

### Option 3. Greedy Or Priority-Based Heuristic

For each recommendation_engine DAG, compute earliest feasible start after dependencies, then assign start slots inside working hours while minimizing local crowding.

Pros:

- fast,
- easy to implement,
- useful before you have reliable runtime duration estimates.

Cons:

- no optimality guarantee,
- can miss better global tradeoffs.

This is a good first operational baseline.

### Option 4. Simulation + Search

Use candidate schedules, simulate the dependency chain with estimated durations, and search over schedules with hill climbing, local search, or Bayesian optimization.

Pros:

- handles realistic delay propagation,
- naturally incorporates uncertain durations later.

Cons:

- computationally heavier,
- harder to prove optimality.

This becomes attractive once you have real actual start/end/duration history.

## Recommended Practical Sequence

### Phase 1. Deterministic rescheduling baseline

Use current dependency graph plus estimated durations to build a CP-SAT or MILP model with:

- hard precedence constraints,
- working-hours constraints on recommendation_engine DAGs only,
- crowding penalty for seed DAG starts,
- moderate penalty for moving too far from today’s cron schedule.

### Phase 2. Historical calibration

Once runtime data is available, estimate:

- actual duration distributions,
- sensor wait distributions,
- typical upstream completion windows,
- critical bottleneck edges.

Then update the objective weights based on observed waiting cost rather than intuition alone.

### Phase 3. Robust or stochastic scheduling

When duration uncertainty is stable enough to estimate, optimize against expected or worst-case waiting rather than deterministic duration estimates.

## What To Optimize First

Given your stated priority, the first optimization target should be:

1. reduce simultaneous release of recommendation_engine DAGs into the same upstream bottlenecks,
2. move those DAG starts into a shared DS team working-hours window,
3. keep schedule shifts moderate unless the wait reduction is materially better.

That means the best first objective is not simply:

$$
\min \sum |\Delta_{ik}|
$$

but instead something closer to:

$$
\min \;
\alpha \cdot \text{downstream waiting}
+ \beta \cdot \text{start-time crowding}
+ \gamma \cdot \text{schedule change}
$$

with $\alpha > \beta > \gamma$ initially.

## Data You Will Need Next Week

To move from formulation to implementation, the highest-value additions are:

- actual DAG start and end times,
- actual durations by DAG and run,
- actual sensor task wait durations,
- mapping from downstream runs to upstream runs by logical date,
- a chosen DS team working-hours window.

With those, the current formulation can be calibrated into a concrete optimizer rather than remaining only structural.
