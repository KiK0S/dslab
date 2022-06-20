use std::cell::RefCell;
use std::rc::Rc;

use dslab_core::cast;
use dslab_core::context::SimulationContext;
use dslab_core::event::Event;
use dslab_core::handler::EventHandler;
use dslab_core::log_debug;

use crate::core::common::Allocation;
use crate::core::config::SimulationConfig;
use crate::core::events::allocation::{
    AllocationCommitFailed, AllocationCommitRequest, AllocationCommitSucceeded, AllocationFailed, AllocationReleased,
    AllocationRequest,
};
use crate::core::monitoring::Monitoring;
use crate::core::resource_pool::ResourcePoolState;
use crate::core::vm::VirtualMachine;
use crate::core::vm_placement_algorithm::VMPlacementAlgorithm;

pub struct Scheduler {
    pub id: u32,
    pool_state: ResourcePoolState,
    placement_store_id: u32,
    monitoring: Rc<RefCell<Monitoring>>,
    vm_placement_algorithm: Box<dyn VMPlacementAlgorithm>,
    ctx: SimulationContext,
    sim_config: Rc<SimulationConfig>,
}

impl Scheduler {
    pub fn new(
        snapshot: ResourcePoolState,
        monitoring: Rc<RefCell<Monitoring>>,
        placement_store_id: u32,
        vm_placement_algorithm: Box<dyn VMPlacementAlgorithm>,
        ctx: SimulationContext,
        sim_config: Rc<SimulationConfig>,
    ) -> Self {
        Self {
            id: ctx.id(),
            pool_state: snapshot,
            placement_store_id,
            monitoring,
            vm_placement_algorithm,
            ctx,
            sim_config,
        }
    }

    pub fn add_host(&mut self, id: u32, cpu_total: u32, memory_total: u64) {
        self.pool_state
            .add_host(id, cpu_total, memory_total, cpu_total, memory_total);
    }

    fn on_allocation_request(&mut self, alloc: Allocation, vm: VirtualMachine) {
        if let Some(host) = self
            .vm_placement_algorithm
            .select_host(&alloc, &self.pool_state, &self.monitoring.borrow())
        {
            log_debug!(
                self.ctx,
                "scheduler #{} decided to pack vm #{} on host #{}",
                self.id,
                alloc.id,
                host
            );
            self.pool_state.allocate(&alloc, host);

            self.ctx.emit(
                AllocationCommitRequest {
                    alloc,
                    vm,
                    host_id: host,
                },
                self.placement_store_id,
                self.sim_config.message_delay,
            );
        } else {
            log_debug!(self.ctx, "scheduler #{} failed to pack vm #{}", self.id, alloc.id,);
            self.ctx
                .emit_self(AllocationRequest { alloc, vm }, self.sim_config.allocation_retry_period);
        }
    }

    fn on_allocation_commit_succeeded(&mut self, alloc: Allocation, host_id: u32) {
        self.pool_state.allocate(&alloc, host_id);
    }

    fn on_allocation_commit_failed(&mut self, alloc: Allocation, host_id: u32) {
        self.pool_state.release(&alloc, host_id);
    }

    fn on_allocation_released(&mut self, alloc: Allocation, host_id: u32) {
        self.pool_state.release(&alloc, host_id);
    }

    fn on_allocation_failed(&mut self, alloc: Allocation, host_id: u32) {
        self.pool_state.release(&alloc, host_id);
    }
}

impl EventHandler for Scheduler {
    fn on(&mut self, event: Event) {
        cast!(match event.data {
            AllocationRequest { alloc, vm } => {
                self.on_allocation_request(alloc, vm);
            }
            AllocationCommitSucceeded { alloc, host_id } => {
                self.on_allocation_commit_succeeded(alloc, host_id);
            }
            AllocationCommitFailed { alloc, host_id } => {
                self.on_allocation_commit_failed(alloc, host_id);
            }
            AllocationReleased { alloc, host_id } => {
                self.on_allocation_released(alloc, host_id);
            }
            AllocationFailed { alloc, host_id } => {
                self.on_allocation_failed(alloc, host_id);
            }
        })
    }
}