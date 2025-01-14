use std::cell::RefCell;
use std::rc::Rc;

use dslab_core::context::SimulationContext;

use crate::event::ContainerStartEvent;
use crate::function::Application;
use crate::resource::{ResourceConsumer, ResourceProvider};
use crate::util::{Counter, DefaultVecMap, FxIndexMap, FxIndexSet};

#[derive(Eq, PartialEq)]
pub enum ContainerStatus {
    Deploying,
    Running,
    Idle,
}

pub struct Container {
    pub status: ContainerStatus,
    pub id: usize,
    pub deployment_time: f64,
    pub app_id: usize,
    pub invocations: FxIndexSet<usize>,
    pub resources: ResourceConsumer,
    pub started_invocations: usize,
    pub last_change: f64,
    pub cpu_share: f64,
}

impl Container {
    pub fn start_invocation(&mut self, id: usize) {
        self.invocations.insert(id);
        self.started_invocations += 1;
    }

    pub fn end_invocation(&mut self, id: usize, curr_time: f64) {
        self.last_change = curr_time;
        self.invocations.remove(&id);
        if self.invocations.is_empty() {
            self.status = ContainerStatus::Idle;
        }
    }
}

pub struct ContainerManager {
    active_invocations: usize,
    resources: ResourceProvider,
    containers: FxIndexMap<usize, Container>,
    containers_by_app: DefaultVecMap<FxIndexSet<usize>>,
    container_counter: Counter,
    reservations: FxIndexMap<usize, Vec<usize>>,
    ctx: Rc<RefCell<SimulationContext>>,
}

impl ContainerManager {
    pub fn new(resources: ResourceProvider, ctx: Rc<RefCell<SimulationContext>>) -> Self {
        Self {
            active_invocations: 0,
            resources,
            containers: FxIndexMap::default(),
            containers_by_app: Default::default(),
            container_counter: Counter::default(),
            reservations: FxIndexMap::default(),
            ctx,
        }
    }

    pub fn can_allocate(&self, resources: &ResourceConsumer) -> bool {
        self.resources.can_allocate(resources)
    }

    pub fn get_total_resource(&self, id: usize) -> u64 {
        self.resources.get_resource(id).unwrap().get_available()
    }

    pub fn dec_active_invocations(&mut self) {
        self.active_invocations -= 1;
    }

    pub fn inc_active_invocations(&mut self) {
        self.active_invocations += 1;
    }

    pub fn active_invocation_count(&self) -> usize {
        self.active_invocations
    }

    pub fn get_container(&self, id: usize) -> Option<&Container> {
        self.containers.get(&id)
    }

    pub fn get_container_mut(&mut self, id: usize) -> Option<&mut Container> {
        self.containers.get_mut(&id)
    }

    pub fn get_containers(&mut self) -> &mut FxIndexMap<usize, Container> {
        &mut self.containers
    }

    pub fn get_possible_containers(&self, app: &Application, allow_deploying: bool) -> PossibleContainerIterator<'_> {
        let limit = app.get_concurrent_invocations();
        if let Some(set) = self.containers_by_app.get(app.id) {
            return PossibleContainerIterator::new(
                Some(set.iter()),
                &self.containers,
                &self.reservations,
                limit,
                allow_deploying,
            );
        }
        PossibleContainerIterator::new(None, &self.containers, &self.reservations, limit, allow_deploying)
    }

    pub fn try_deploy(&mut self, app: &Application, time: f64) -> Option<(usize, f64)> {
        if self.resources.can_allocate(app.get_resources()) {
            let id = self.deploy_container(app, time);
            return Some((id, app.get_deployment_time()));
        }
        None
    }

    pub fn reserve_container(&mut self, id: usize, request: usize) {
        self.reservations.entry(id).or_default().push(request);
    }

    pub fn take_reservations(&mut self, id: usize) -> Option<Vec<usize>> {
        self.reservations.remove(&id)
    }

    pub fn delete_container(&mut self, id: usize) {
        let container = self.containers.remove(&id).unwrap();
        self.containers_by_app.get_mut(container.app_id).remove(&id);
        self.resources.release(&container.resources);
    }

    fn deploy_container(&mut self, app: &Application, time: f64) -> usize {
        let cont_id = self.container_counter.increment();
        let container = Container {
            status: ContainerStatus::Deploying,
            id: cont_id,
            deployment_time: app.get_deployment_time(),
            app_id: app.id,
            invocations: Default::default(),
            resources: app.get_resources().clone(),
            started_invocations: 0,
            last_change: time,
            cpu_share: app.get_cpu_share(),
        };
        self.resources.allocate(&container.resources);
        self.containers.insert(cont_id, container);
        self.containers_by_app.get_mut(app.id).insert(cont_id);
        self.ctx
            .borrow_mut()
            .emit_self(ContainerStartEvent { id: cont_id }, app.get_deployment_time());
        cont_id
    }
}

pub struct PossibleContainerIterator<'a> {
    inner: Option<indexmap::set::Iter<'a, usize>>,
    containers: &'a FxIndexMap<usize, Container>,
    reserve: &'a FxIndexMap<usize, Vec<usize>>,
    limit: usize,
    allow_deploying: bool,
}

impl<'a> PossibleContainerIterator<'a> {
    pub fn new(
        inner: Option<indexmap::set::Iter<'a, usize>>,
        containers: &'a FxIndexMap<usize, Container>,
        reserve: &'a FxIndexMap<usize, Vec<usize>>,
        limit: usize,
        allow_deploying: bool,
    ) -> Self {
        Self {
            inner,
            containers,
            reserve,
            limit,
            allow_deploying,
        }
    }
}

impl<'a> Iterator for PossibleContainerIterator<'a> {
    type Item = &'a Container;
    fn next(&mut self) -> Option<Self::Item> {
        if let Some(inner) = self.inner.as_mut() {
            for id in inner.by_ref() {
                let c = self.containers.get(id).unwrap();
                if c.status != ContainerStatus::Deploying && c.invocations.len() < self.limit {
                    return Some(c);
                }
                if c.status == ContainerStatus::Deploying
                    && self.allow_deploying
                    && (!self.reserve.contains_key(id) || self.reserve.get(id).unwrap().len() < self.limit)
                {
                    return Some(c);
                }
            }
            return None;
        }
        None
    }
}
