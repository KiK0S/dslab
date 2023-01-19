use dslab_core::component::Id;
use dslab_core::event::Event;
use dslab_core::event::EventId;
use rand::prelude::SliceRandom;
use serde::Serialize;
use std::cell::RefCell;
use std::collections::HashMap;
use std::fmt::Debug;
use std::rc::Rc;
use std::vec::Vec;

#[derive(Debug)]
struct DependencyWrapper<T: Copy + PartialEq + Debug> {
    pub inner: T,
    dependencies_before: Vec<Rc<RefCell<DependencyWrapper<T>>>>,
    dependencies_after: Vec<Rc<RefCell<DependencyWrapper<T>>>>,
}

struct TimerDependencyResolver {
    node_timers: HashMap<Id, Vec<(f64, Rc<RefCell<DependencyWrapper<EventId>>>)>>,
    event_to_node: HashMap<EventId, Id>,
}

pub struct DependencyResolver {
    available_events: Vec<Rc<RefCell<DependencyWrapper<EventId>>>>,
    timer_resolver: TimerDependencyResolver,
}

impl TimerDependencyResolver {
    pub fn new() -> Self {
        TimerDependencyResolver {
            node_timers: HashMap::new(),
            event_to_node: HashMap::new(),
        }
    }
    pub fn add(&mut self, node: Id, time: f64, event: Rc<RefCell<DependencyWrapper<EventId>>>) {
        assert!(
            self.event_to_node.insert(event.as_ref().borrow().inner, node).is_none(),
            "duplicate EventId not allowed"
        );
        let timers = self.node_timers.entry(node).or_default();
        let mut max_time_before_idx = None;
        if let Some(first_timer) = timers.first() {
            if first_timer.0 > time {
                max_time_before_idx = Some(-1);
            }
        }
        for (idx, timer) in timers.iter().enumerate() {
            if timer.0 < time {
                max_time_before_idx = Some(idx as i32);
            }
        }
        if let Some(idx) = max_time_before_idx {
            let (before, after) = timers.split_at((idx + 1) as usize);
            // theoretically, dependency before <-> after should be deleted, but it can be deleted later because it is always O(1) extra
            *timers = before
                .into_iter()
                .cloned()
                .chain(vec![(time, event.clone())].iter().cloned())
                .chain(after.into_iter().cloned())
                .collect();
            if idx >= 0 {
                timers[idx as usize].1.as_ref().borrow_mut().add_child(&event);
                event.as_ref().borrow_mut().add_parent(&timers[idx as usize].1);
            }
            if idx + 2 < timers.len() as i32 {
                timers[(idx + 2) as usize].1.as_ref().borrow_mut().add_parent(&event);
                event.as_ref().borrow_mut().add_child(&timers[(idx + 2) as usize].1);
            }
        } else {
            if let Some(last_event) = timers.last() {
                last_event.1.as_ref().borrow_mut().add_child(&event);
                event.as_ref().borrow_mut().add_parent(&last_event.1);
            }
            timers.push((time, event));
        }
    }

    pub fn pop(&mut self, event_id: EventId) {
        let node = self.event_to_node.remove(&event_id).unwrap();
        let data = self.node_timers.get_mut(&node).unwrap();
        assert!(data.len() > 0, "cannot pop from empty vector");
        let mut event_pos = None;
        for (idx, elem) in data.iter().enumerate() {
            if elem.1.as_ref().borrow().inner == event_id {
                event_pos = Some(idx);
            }
        }
        assert!(event_pos.is_some());
        let event_pos = event_pos.unwrap();
        assert!(data[event_pos].0 == data[0].0);
        if event_pos > 0 && event_pos + 1 < data.len() {
            // need to link over removed elem
            let a = &data[event_pos - 1].1;
            let b = &data[event_pos + 1].1;
            a.as_ref().borrow_mut().add_child(&b);
            b.as_ref().borrow_mut().add_parent(&a);
        }
        data.remove(event_pos);
    }
}

impl DependencyResolver {
    pub fn new() -> Self {
        DependencyResolver {
            available_events: Vec::default(),
            timer_resolver: TimerDependencyResolver::new(),
        }
    }

    pub fn add_event(&mut self, event: &Event) {
        let dependent_event = Rc::new(RefCell::new(DependencyWrapper::<EventId>::new(event.id)));

        let time = event.time;

        self.timer_resolver.add(event.src, time, dependent_event.clone());
        if dependent_event.as_ref().borrow().is_available() {
            self.available_events.push(dependent_event);
        }
        // earlier events can now be blocked
        self.available_events
            .retain(|elem| elem.as_ref().borrow().is_available());
    }

    pub fn available_events(&self) -> Vec<EventId> {
        self.available_events
            .iter()
            .map(|event| event.as_ref().borrow().inner)
            .collect()
    }

    pub fn pop_event(&mut self, event_id: EventId) {
        let event = self
            .available_events
            .iter()
            .enumerate()
            .find(|x| x.1.as_ref().borrow().inner == event_id)
            .unwrap();
        let next_events = event.1.as_ref().borrow_mut().pop_dependencies();
        for dependency in next_events.iter() {
            let idx = dependency
                .as_ref()
                .borrow()
                .dependencies_before
                .iter()
                .enumerate()
                .find(|elem| elem.1.as_ref().borrow().inner == event.1.as_ref().borrow().inner)
                .unwrap()
                .0;
            dependency.borrow_mut().dependencies_before.remove(idx);
        }
        self.available_events.remove(event.0);
        for dependency in next_events.iter() {
            if dependency.as_ref().borrow().is_available() {
                self.available_events.push(dependency.clone());
            }
        }
        self.timer_resolver.pop(event_id);
    }
}

impl<T: Copy + PartialEq + Debug> DependencyWrapper<T> {
    pub fn new(inner: T) -> Self {
        DependencyWrapper {
            inner,
            dependencies_before: Vec::new(),
            dependencies_after: Vec::new(),
        }
    }

    pub fn add_parent(&mut self, other: &Rc<RefCell<DependencyWrapper<T>>>) {
        self.dependencies_before.push(other.clone());
    }
    pub fn add_child(&mut self, other: &Rc<RefCell<DependencyWrapper<T>>>) {
        self.dependencies_after.push(other.clone());
    }

    pub fn pop_dependencies(&mut self) -> Vec<Rc<RefCell<DependencyWrapper<T>>>> {
        self.dependencies_after.drain(..).collect()
    }

    pub fn is_available(&self) -> bool {
        self.dependencies_before.is_empty()
    }
}

#[derive(Serialize)]
struct SamplePayload {}

#[test]
fn test_dependency_resolver_simple() {
    let mut resolver = DependencyResolver::new();
    let mut sequence = Vec::new();
    for node_id in 0..3 {
        let mut times: Vec<u64> = (0..3).into_iter().collect();
        times.shuffle(&mut rand::thread_rng());
        for event_time in times {
            let event = Event {
                id: event_time * 3 + node_id,
                src: node_id as u32,
                dest: 0,
                time: event_time as f64,
                data: Box::new(SamplePayload {}),
            };
            resolver.add_event(&event);
        }
    }
    while let Some(id) = resolver.available_events().choose(&mut rand::thread_rng()) {
        let id = *id;
        sequence.push(id);
        resolver.pop_event(id);
    }
    println!("{:?}", sequence);
    assert!(sequence.len() == 9);
    let mut timers = vec![0, 0, 0];
    for event_id in sequence {
        let time = event_id / 3;
        let node = event_id % 3;
        assert!(timers[node as usize] == time);
        timers[node as usize] += 1;
    }
}

#[test]
fn test_dependency_resolver_pop() {
    let mut resolver = DependencyResolver::new();
    let mut sequence = Vec::new();
    for node_id in 0..3 {
        let mut times: Vec<u64> = (0..3).into_iter().collect();
        times.shuffle(&mut rand::thread_rng());
        for event_time in times {
            let event = Event {
                id: event_time * 3 + node_id,
                src: node_id as u32,
                dest: 0,
                time: event_time as f64,
                data: Box::new(SamplePayload {}),
            };
            resolver.add_event(&event);
        }
    }
    for _ in 0..7 {
        let id = *resolver.available_events().choose(&mut rand::thread_rng()).unwrap();
        sequence.push(id);
        resolver.pop_event(id);
    }
    for node_id in 0..3 {
        let event = Event {
            id: 9 + node_id,
            src: node_id as u32,
            dest: 0,
            time: 3.0,
            data: Box::new(SamplePayload {}),
        };
        resolver.add_event(&event);
    }
    while let Some(id) = resolver.available_events().choose(&mut rand::thread_rng()) {
        let id = *id;
        sequence.push(id);
        resolver.pop_event(id);
    }
    println!("{:?}", sequence);
    assert!(sequence.len() == 12);
    let mut timers = vec![0, 0, 0];
    for event_id in sequence {
        let time = event_id / 3;
        let node = event_id % 3;
        assert!(timers[node as usize] == time);
        timers[node as usize] += 1;
    }
}
