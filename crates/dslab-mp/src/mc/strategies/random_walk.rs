//! Implementation of Random Walk search strategy.

use std::collections::HashSet;

use colored::*;
use rand::seq::IteratorRandom;

use crate::mc::strategy::{
    CollectFn, ExecutionMode, GoalFn, InvariantFn, McResult, McSummary, PruneFn, Strategy, VisitedStates,
};
use crate::mc::system::{McState, McSystem};
use crate::util::t;

/// The search strategy based on random walk.
pub struct RandomWalk<'a> {
    prune: PruneFn<'a>,
    goal: GoalFn<'a>,
    invariant: InvariantFn<'a>,
    collect: Option<CollectFn<'a>>,
    execution_mode: ExecutionMode,
    summary: McSummary,
    visited: VisitedStates,
    collected: HashSet<McState>,
    walks: usize,
    max_depth: u64,
}

impl<'a> RandomWalk<'a> {
    /// Creates a new Dfs instance with specified user-defined functions and execution mode.
    pub fn new(
        prune: PruneFn<'a>,
        goal: GoalFn<'a>,
        invariant: InvariantFn<'a>,
        collect: Option<CollectFn<'a>>,
        execution_mode: ExecutionMode,
        walks: usize,
        max_depth: u64,
    ) -> Self {
        let visited = Self::initialize_visited(&execution_mode);
        Self {
            prune,
            goal,
            invariant,
            collect,
            execution_mode,
            summary: McSummary::default(),
            visited,
            collected: HashSet::new(),
            walks,
            max_depth,
        }
    }
}

impl<'a> RandomWalk<'a> {
    fn walk(&mut self, system: &mut McSystem, state: McState) -> Result<(), String> {
        let available_events = system.available_events();
        let cur_depth = state.search_depth;
        let result = self.check_state(&state);

        if let Some(result) = result {
            return result;
        }

        let next_event_id = available_events.iter().choose(&mut rand::thread_rng());
        if let Some(next_event_id) = next_event_id {
            if cur_depth <= self.max_depth {
                self.process_event(system, *next_event_id)?;
            }
        }
        Ok(())
    }
}

impl<'a> Strategy for RandomWalk<'a> {
    fn run(&mut self, system: &mut McSystem) -> Result<McResult, String> {
        let state = system.get_state();
        for _ in 0..self.walks {
            self.walk(system, state.clone())?;
        }
        Ok(McResult {
            summary: self.summary.clone(),
            collected: self.collected.clone(),
        })
    }

    fn search_step_impl(&mut self, system: &mut McSystem, state: McState) -> Result<(), String> {
        self.walk(system, state)
    }

    fn execution_mode(&self) -> &ExecutionMode {
        &self.execution_mode
    }

    fn visited(&mut self) -> &mut VisitedStates {
        &mut self.visited
    }

    fn prune(&self) -> &PruneFn {
        &self.prune
    }

    fn goal(&self) -> &GoalFn {
        &self.goal
    }

    fn invariant(&self) -> &InvariantFn {
        &self.invariant
    }

    fn collect(&self) -> &Option<CollectFn> {
        &self.collect
    }

    fn collected(&mut self) -> &mut HashSet<McState> {
        &mut self.collected
    }

    fn summary(&mut self) -> &mut McSummary {
        &mut self.summary
    }
}
