#! /usr/bin/env python

import os
import shutil

import custom_parser

import project


REPO = project.get_repo_base()
BENCHMARKS_DIR = os.environ["DOWNWARD_BENCHMARKS"]
SCP_LOGIN = "myname@myserver.com"
REMOTE_REPOS_DIR = "/infai/seipp/projects"
# If REVISION_CACHE is None, the default "./data/revision-cache/" is used.
REVISION_CACHE = os.environ.get("DOWNWARD_REVISION_CACHE")
# Since the runs folders are created by the build step, which requires cmake and therefore cannot be run on the cluster submission machine, the following logic does not work:
# if project.REMOTE:
#     SUITE = project.SUITE_SATISFICING
#     ENV = project.UnhSlurmEnvironment(email="myname@myserver.com")
# else:
#     SUITE = ["depot:p01.pddl", "grid:prob01.pddl", "gripper:prob01.pddl"]
#     ENV = project.LocalEnvironment(processes=2)
# ...Instead, we just always want to use the full suite:

# Small suite for testing:
SUITE = [
    "depot:p01.pddl",
    "grid:prob01.pddl",
    "gripper:prob01.pddl",
]
# Reasonably sized suite for actual experiments:
# SUITE = project.SUITE_IPC14_SAT
with open(os.path.join(os.path.expanduser("~"), "email"), "r") as f:
    email = f.read().strip()
print(f"Will send Slurm notifications to '{email}'")

# 15G mem limit allows four jobs to run on each 4-core, 64GB machine:
ENV = project.UnhSlurmEnvironment(email=email, real_memory_required_per_node="15G")
# ...Of course, this will cause problems when the start_runs step is called locally instead of from ai0.

# example command:
# ./fast-downward.py ../downward-benchmarks/gripper/prob01.pddl --evaluator "h=ff(transform=adapt_costs(one))" --search "eager(ucb1power(h), check_goal_early=true, cost_type=one, min_time=300, min_eval=10000, min_exp=4000)"
HEURISTICS = [
    ("ff", "ff"),
    ("add", "add"),
    ("hmax", "hmax"),
    ("goalcount", "goalcount"),
    ("cea", "cea"),
]
COST_TYPE_AND_LIMITS = "cost_type=one, min_time=180, min_eval=10000, min_exp=4000"
UNIT_COST = "transform=adapt_costs(one)"
CONFIGS = [
    (f"{index}-{alg_nick}", alg_config)
    for (index, (alg_nick, alg_config)) in enumerate(
        # mcts
        [
            (
                f"{o_nick}-{h_nick}",
                [
                    "--evaluator",
                    f"h={h}({UNIT_COST})",
                    "--search",
                    f"eager({o}(h), check_goal_early=true, {COST_TYPE_AND_LIMITS})",
                ],
            )
            for (o_nick, o) in [
                ("thts_bfs", "thts_bfs"),
                ("ucb1", "ucb1"),
                ("ucb1normal", "ucb1normal"),
                ("ucb1normal2", "ucb1normal2"),
                ("ucb1power", "ucb1power"),
                ("ucb1uniform", "ucb1uniform"),
            ]
            for (h_nick, h) in HEURISTICS
        ]
        # gbfs
        + [
            (
                f"gbfs-{h_nick}",
                [
                    "--search",
                    f"lazy_greedy([{h}({UNIT_COST})], {COST_TYPE_AND_LIMITS})",
                ],
            )
            for (h_nick, h) in HEURISTICS
        ]
        # wastar
        + [
            (
                f"wastar5-{h_nick}",
                [
                    "--search",
                    f"eager_wastar([{h}({UNIT_COST})], w=5, check_goal_early=false, {COST_TYPE_AND_LIMITS})",
                ],
            )
            for (h_nick, h) in HEURISTICS
        ],
        start=1,
    )
]
# # debug
# for c in CONFIGS:
#     print(c)
# quit()

BUILD_OPTIONS = []
DRIVER_OPTIONS = ["--translate-time-limit", "5m"]
REV_NICKS = [
    # commit c66e8d8fbba12c1b59301e0b347939790c3035bc
    # Author: Stephen Josef Wissow <sjw@cs.unh.edu>
    # Date:   Mon Mar 18 12:50:55 2024 -0400
    #
    #     support wastar
    ("c66e8d8fbba12c1b59301e0b347939790c3035bc", "c66e8d"),
]
ATTRIBUTES = [
    "error",
    "run_dir",
    "search_start_time",
    "search_start_memory",
    "total_time",
    "h_values",
    "coverage",
    "expansions",
    "memory",
    project.EVALUATIONS_PER_TIME,
]

exp = project.FastDownwardExperiment(environment=ENV, revision_cache=REVISION_CACHE)
for config_nick, config in CONFIGS:
    for rev, rev_nick in REV_NICKS:
        algo_name = f"{rev_nick}:{config_nick}" if rev_nick else config_nick
        exp.add_algorithm(
            algo_name,
            REPO,
            rev,
            config,
            build_options=BUILD_OPTIONS,
            driver_options=DRIVER_OPTIONS,
        )
exp.add_suite(BENCHMARKS_DIR, SUITE)

exp.add_parser(exp.EXITCODE_PARSER)
exp.add_parser(exp.TRANSLATOR_PARSER)
exp.add_parser(exp.SINGLE_SEARCH_PARSER)
exp.add_parser(custom_parser.get_parser())
exp.add_parser(exp.PLANNER_PARSER)

exp.add_step("build", exp.build)
exp.add_step("start", exp.start_runs)
exp.add_step("parse", exp.parse)
exp.add_fetcher(name="fetch")

if not project.REMOTE:
    # TODO maybe don't want this
    exp.add_step("remove-eval-dir", shutil.rmtree, exp.eval_dir, ignore_errors=True)
    # project.add_scp_step(exp, SCP_LOGIN, REMOTE_REPOS_DIR)

project.add_absolute_report(
    exp, attributes=ATTRIBUTES, filter=[project.add_evaluations_per_time]
)

attributes = ["expansions"]
pairs = [
    ("16-ucb1normal2-ff", "26-ucb1uniform-ff"),
    # Won't need all these, but maybe helpful for validation initially.
    # (a[0], b[0])
    # for a in CONFIGS
    # for b in CONFIGS
    # if "ff" in a[0] and "ff" in b[0] and a[0] != b[0]
]
# # debug
# for (i, p) in enumerate(pairs, start=1):
#     print(f"{i}: {p}")
suffix = "-rel" if project.RELATIVE else ""
for algo1, algo2 in pairs:
    for attr in attributes:
        exp.add_report(
            project.ScatterPlotReport(
                relative=project.RELATIVE,
                get_category=None if project.TEX else lambda run1, run2: run1["domain"],
                attributes=[attr],
                filter_algorithm=[algo1, algo2],
                filter=[project.add_evaluations_per_time],
                format="tex" if project.TEX else "png",
            ),
            name=f"{exp.name}-{algo1}-vs-{algo2}-{attr}{suffix}",
        )

exp.run_steps()
