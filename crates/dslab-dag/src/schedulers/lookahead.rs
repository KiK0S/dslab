use std::collections::{BTreeSet, HashSet};

use dslab_core::context::SimulationContext;
use dslab_core::{log_debug, log_info, log_warn};

use crate::dag::DAG;
use crate::data_item::{DataTransferMode, DataTransferStrategy};
use crate::runner::Config;
use crate::scheduler::{Action, Scheduler};
use crate::schedulers::common::{calc_ranks, evaluate_assignment, predecessors, ScheduledTask};
use crate::system::System;
use crate::task::*;

pub struct LookaheadScheduler {
    data_transfer_strategy: DataTransferStrategy,
}

impl LookaheadScheduler {
    pub fn new() -> Self {
        LookaheadScheduler {
            data_transfer_strategy: DataTransferStrategy::Eager,
        }
    }

    pub fn with_data_transfer_strategy(mut self, data_transfer_strategy: DataTransferStrategy) -> Self {
        self.data_transfer_strategy = data_transfer_strategy;
        self
    }
}

impl Scheduler for LookaheadScheduler {
    fn start(&mut self, dag: &DAG, system: System, config: Config, ctx: &SimulationContext) -> Vec<Action> {
        assert_ne!(
            config.data_transfer_mode,
            DataTransferMode::Manual,
            "LookaheadScheduler doesn't support DataTransferMode::Manual"
        );

        if dag.get_tasks().iter().any(|task| task.min_cores != task.max_cores) {
            log_warn!(
                ctx,
                "some tasks support different number of cores, but HEFT will always use min_cores"
            );
        }

        let resources = system.resources;
        let network = system.network;

        let data_transfer_mode = &config.data_transfer_mode;

        let avg_net_time = system.avg_net_time(ctx.id(), data_transfer_mode);
        let avg_upload_net_time = system.avg_upload_net_time(ctx.id());

        let total_tasks = dag.get_tasks().len();

        let pred = predecessors(dag);

        let task_ranks = calc_ranks(system.avg_flop_time(), avg_net_time, dag);
        let mut tasks = (0..total_tasks).collect::<Vec<_>>();
        tasks.sort_by(|&a, &b| task_ranks[b].total_cmp(&task_ranks[a]));

        let mut scheduled_tasks = resources
            .iter()
            .map(|resource| {
                (0..resource.cores_available)
                    .map(|_| BTreeSet::<ScheduledTask>::new())
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        let mut eft = vec![0.; total_tasks];
        let mut scheduled = vec![false; total_tasks];

        let mut result: Vec<(f64, Action)> = Vec::new();

        let inputs: HashSet<usize> = dag
            .get_tasks()
            .iter()
            .flat_map(|task| task.inputs.iter())
            .cloned()
            .collect();
        let outputs: HashSet<usize> = dag
            .get_tasks()
            .iter()
            .flat_map(|task| task.outputs.iter())
            .cloned()
            .collect();

        let get_task_est = |task: usize, eft: &Vec<f64>| -> f64 {
            match self.data_transfer_strategy {
                DataTransferStrategy::Eager => pred[task]
                    .iter()
                    .map(|&(task, weight)| eft[task] + weight * avg_net_time)
                    .max_by(|a, b| a.total_cmp(&b))
                    .unwrap_or(0.),
                DataTransferStrategy::Lazy => pred[task]
                    .iter()
                    .map(|&(task, weight)| {
                        let data_upload_time = match data_transfer_mode {
                            DataTransferMode::ViaMasterNode => weight * avg_upload_net_time,
                            DataTransferMode::Direct => 0.,
                            DataTransferMode::Manual => 0.,
                        };
                        eft[task] + data_upload_time
                    })
                    .max_by(|a, b| a.total_cmp(&b))
                    .unwrap_or(0.),
            }
        };

        for task in tasks.into_iter() {
            let est = get_task_est(task, &eft);

            let mut best_eft = -1.;
            let mut best_time = -1.;
            let mut best_finish = -1.;
            let mut best_resource = 0 as usize;
            let mut best_cores: Vec<u32> = Vec::new();
            for resource in 0..resources.len() {
                let res = evaluate_assignment(
                    task,
                    resource,
                    est,
                    &scheduled_tasks,
                    &outputs,
                    avg_net_time,
                    &self.data_transfer_strategy,
                    dag,
                    resources,
                    network,
                    &config,
                    ctx,
                );
                if res.is_none() {
                    continue;
                }
                let (est, time, cores) = res.unwrap();

                let mut to_undo: Vec<(usize, Vec<u32>, ScheduledTask)> = Vec::new();

                for &core in cores.iter() {
                    scheduled_tasks[resource][core as usize].insert(ScheduledTask::new(est, est + time, task));
                }
                eft[task] = est + time;
                scheduled[task] = true;
                to_undo.push((resource, cores.clone(), ScheduledTask::new(est, est + time, task)));

                let mut children = (0..total_tasks)
                    .filter(|&task| !scheduled[task])
                    .collect::<Vec<usize>>();
                children.sort_by(|&a, &b| task_ranks[b].total_cmp(&task_ranks[a]));
                let mut max_eft = est + time;
                for &child in children.iter() {
                    let (resource, cores, finish, time) = {
                        let task = child;
                        let est = get_task_est(task, &eft);

                        let mut best_finish = -1.;
                        let mut best_time = -1.;
                        let mut best_resource = 0 as usize;
                        let mut best_cores: Vec<u32> = Vec::new();
                        for resource in 0..resources.len() {
                            let res = evaluate_assignment(
                                task,
                                resource,
                                est,
                                &scheduled_tasks,
                                &outputs,
                                avg_net_time,
                                &self.data_transfer_strategy,
                                dag,
                                resources,
                                network,
                                &config,
                                ctx,
                            );
                            if res.is_none() {
                                continue;
                            }
                            let (est, time, cores) = res.unwrap();

                            if best_finish == -1. || best_finish > est + time {
                                best_time = time;
                                best_finish = est + time;
                                best_resource = resource;
                                best_cores = cores;
                            }
                        }

                        assert!(best_finish != -1.);

                        (best_resource, best_cores, best_finish, best_time)
                    };
                    for &core in cores.iter() {
                        scheduled_tasks[resource][core as usize].insert(ScheduledTask::new(
                            finish - time,
                            finish,
                            child,
                        ));
                    }
                    eft[child] = finish;
                    scheduled[child] = true;
                    to_undo.push((resource, cores, ScheduledTask::new(finish - time, finish, child)));
                    if finish > max_eft {
                        max_eft = finish;
                    }
                }

                for (resource, cores, scheduled_task) in to_undo.into_iter() {
                    for &core in cores.iter() {
                        assert!(scheduled_tasks[resource][core as usize].remove(&scheduled_task));
                    }
                    scheduled[scheduled_task.task] = false;
                }

                if best_eft == -1. || best_eft > max_eft {
                    best_time = time;
                    best_finish = est + time;
                    best_eft = max_eft;
                    best_resource = resource;
                    best_cores = cores.clone();
                }
            }

            assert!(best_finish != -1.);

            log_debug!(
                ctx,
                "scheduling [lookahead] task {} on resource {} on cores {:?} on time {:.3}-{:.3}",
                dag.get_task(task).name,
                resources[best_resource].name,
                best_cores,
                best_finish - best_time,
                best_finish
            );

            for &core in best_cores.iter() {
                scheduled_tasks[best_resource][core as usize].insert(ScheduledTask::new(
                    best_finish - best_time,
                    best_finish,
                    task,
                ));
            }
            eft[task] = best_finish;
            scheduled[task] = true;
            result.push((
                best_finish - best_time,
                Action::ScheduleTaskOnCores {
                    task,
                    resource: best_resource,
                    cores: best_cores,
                },
            ));
        }

        log_info!(
            ctx,
            "expected makespan: {:.3}",
            scheduled_tasks
                .iter()
                .enumerate()
                .map(|(resource, cores)| {
                    let cur_net_time = 1. / network.bandwidth(resources[resource].id, ctx.id());
                    cores
                        .iter()
                        .map(|schedule| {
                            schedule.iter().next_back().map_or(0., |task| {
                                task.end_time
                                    + dag
                                        .get_task(task.task)
                                        .outputs
                                        .iter()
                                        .filter(|f| !inputs.contains(f))
                                        .map(|&f| dag.get_data_item(f).size as f64 * cur_net_time)
                                        .max_by(|a, b| a.total_cmp(&b))
                                        .unwrap_or(0.)
                            })
                        })
                        .max_by(|a, b| a.total_cmp(&b))
                        .unwrap_or(0.)
                })
                .max_by(|a, b| a.total_cmp(&b))
                .unwrap_or(0.)
        );

        result.sort_by(|a, b| a.0.total_cmp(&b.0));
        result.into_iter().map(|(_, b)| b).collect()
    }

    fn on_task_state_changed(
        &mut self,
        _task: usize,
        _task_state: TaskState,
        _dag: &DAG,
        _system: System,
        _ctx: &SimulationContext,
    ) -> Vec<Action> {
        Vec::new()
    }
}