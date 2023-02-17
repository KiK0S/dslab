use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::ops::Bound::{Excluded, Unbounded};

use dslab_core::cast;
use ordered_float::OrderedFloat;
use serde::Serialize;

use crate::events::{MessageReceived, TimerFired};
use dslab_core::component::Id;
use dslab_core::event::Event;
use dslab_core::event::EventId;

/// Tracks and enforces dependencies between the TimerFired events on the same node.
/// Any timer with time T should be fired before any timer with time T+x.  
#[derive(Default)]
struct TimerDependencyResolver {
    node_timers: HashMap<Id, BTreeMap<OrderedFloat<f64>, HashSet<EventId>>>,
    event_to_node: HashMap<EventId, Id>,
}

#[derive(Eq, PartialEq, Default)]
enum NetworkMode {
    #[default]
    NetworkStable,
    NetworkUnstable,
}

impl TimerDependencyResolver {
    pub fn add(&mut self, node: Id, time: f64, event: EventId) -> (bool, Option<HashSet<EventId>>) {
        assert!(
            self.event_to_node.insert(event, node).is_none(),
            "duplicate EventId not allowed"
        );
        let timers = self.node_timers.entry(node).or_default();
        timers.entry(OrderedFloat(time)).or_default().insert(event);

        let prev_time = timers.range(..OrderedFloat(time)).next_back();
        let is_available = prev_time.is_none();

        let next_time = timers.range((Excluded(OrderedFloat(time)), Unbounded)).next();
        let blocked_events = next_time.map(|e| e.1).cloned();

        (is_available, blocked_events)
    }

    pub fn pop(&mut self, event: EventId) -> Option<HashSet<EventId>> {
        let node = self.event_to_node.remove(&event).unwrap();
        let timers = self.node_timers.get_mut(&node).unwrap();
        let (_, events) = timers.iter_mut().next().unwrap();
        assert!(events.remove(&event), "event to pop was not first in queue");
        if events.is_empty() {
            timers.pop_first();
            if let Some((_, next_events)) = timers.iter().next() {
                return Some(next_events.clone());
            }
        }
        None
    }
}

#[derive(Default)]
pub struct DependencyResolver {
    available_events: HashSet<EventId>,
    timer_resolver: TimerDependencyResolver,
    network_mode: NetworkMode,
}

impl DependencyResolver {
    pub fn new() -> Self {
        DependencyResolver {
            available_events: HashSet::default(),
            timer_resolver: TimerDependencyResolver::default(),
            network_mode: NetworkMode::NetworkStable,
        }
    }

    pub fn add_event(&mut self, event: Event) {
        cast!(match event.data {
            MessageReceived { msg, src, dest } => {
                if self.network_mode == NetworkMode::NetworkUnstable {
                    return;
                }
                let (is_available, blocked_events) = self.timer_resolver.add(event.dest, event.time + 1.0, event.id);
                if is_available {
                    self.available_events.insert(event.id);
                }
                if let Some(blocked) = blocked_events {
                    self.available_events.retain(|e| !blocked.contains(e));
                }
            }
            TimerFired { proc, timer } => {
                let (is_available, blocked_events) = self.timer_resolver.add(event.src, event.time, event.id);
                if is_available {
                    self.available_events.insert(event.id);
                }
                if let Some(blocked) = blocked_events {
                    self.available_events.retain(|e| !blocked.contains(e));
                }
            }
        });
    }

    pub fn available_events(&self) -> &HashSet<EventId> {
        &self.available_events
    }

    pub fn pop_event(&mut self, event_id: EventId) {
        assert!(self.available_events.remove(&event_id));
        if let Some(unblocked_events) = self.timer_resolver.pop(event_id) {
            self.available_events.extend(unblocked_events);
        };
    }
}

#[cfg(test)]
mod tests {
    use crate::message::Message;

    use super::*;
    use rand::prelude::IteratorRandom;
    use rand::prelude::SliceRandom;

    #[test]
    fn test_ordered_float() {
        let a = OrderedFloat(0.0);
        let b = OrderedFloat(0.0);
        assert!(b <= a);
        assert!(a <= b);
        assert!(a == b);
    }

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
                    data: Box::new(TimerFired {
                        proc: "0".to_owned(),
                        timer: format!("{}", event_time),
                    }),
                };
                resolver.add_event(event);
            }
        }
        while let Some(id) = resolver.available_events().iter().choose(&mut rand::thread_rng()) {
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
                    data: Box::new(TimerFired {
                        proc: "0".to_owned(),
                        timer: format!("{}", event_time),
                    }),
                };
                resolver.add_event(event);
            }
        }

        // remove most of elements
        // timer resolver should clear its queues before it
        // can add next events without broken dependencies
        for _ in 0..7 {
            let id = *resolver
                .available_events()
                .iter()
                .choose(&mut rand::thread_rng())
                .unwrap();
            sequence.push(id);
            resolver.pop_event(id);
        }
        for node_id in 0..3 {
            let event = Event {
                id: 9 + node_id,
                src: node_id as u32,
                dest: 0,
                time: 3.0,
                data: Box::new(TimerFired {
                    proc: "0".to_owned(),
                    timer: "0".to_owned(),
                }),
            };
            resolver.add_event(event);
        }
        while let Some(id) = resolver.available_events().iter().choose(&mut rand::thread_rng()) {
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

    #[test]
    fn test_timer_dependency_resolver_same_time() {
        let mut resolver = DependencyResolver::new();
        let mut sequence = Vec::new();
        for node_id in 0..1 {
            let mut times: Vec<u64> = (0..100).into_iter().collect();
            times.shuffle(&mut rand::thread_rng());
            for event_time in times {
                println!("{}", event_time);
                let event = Event {
                    id: event_time,
                    src: node_id as u32,
                    dest: 0,
                    time: (event_time / 5) as f64,
                    data: Box::new(TimerFired {
                        proc: "0".to_owned(),
                        timer: format!("{}", event_time),
                    }),
                };
                resolver.add_event(event);
            }
        }
        while let Some(id) = resolver.available_events().iter().choose(&mut rand::thread_rng()) {
            println!("{:?}", resolver.available_events());
            let id = *id;
            println!("{}", id);
            sequence.push(id);
            resolver.pop_event(id);
        }
        let mut timers = vec![0];
        for event_id in sequence {
            let time = event_id / 5;
            let node = 0;
            assert!(timers[node as usize] <= time);
            timers[node as usize] = time;
        }
    }

    #[test]
    fn test_timer_dependency_resolver_stable_network() {
        let mut resolver = DependencyResolver::new();
        let mut sequence = Vec::new();
        let times: Vec<u64> = (0..20).into_iter().collect();
        for event_time in times {
            println!("{}", event_time);
            let time = event_time.clamp(0, 11);
            if time == 10 {
                continue;
            }
            let event = Event {
                id: event_time,
                src: 0,
                dest: 0,
                time: time as f64,
                data: Box::new(TimerFired {
                    proc: "0".to_owned(),
                    timer: format!("{}", event_time),
                }),
            };
            resolver.add_event(event);
        }
        let message_times: Vec<u64> = (1..10).step_by(2).into_iter().collect();
        for message_time in message_times {
            println!("{}", message_time);
            let event = Event {
                id: message_time + 100,
                src: 0,
                dest: 0,
                time: message_time as f64,
                data: Box::new(MessageReceived {
                    msg: Message {
                        tip: "a".to_owned(),
                        data: "hello".to_owned(),
                    },
                    src: "0".to_owned(),
                    dest: "0".to_owned(),
                }),
            };
            resolver.add_event(event);
        }
        assert!(resolver.available_events().len() <= 2);
        while let Some(id) = resolver.available_events().iter().choose(&mut rand::thread_rng()) {
            println!("{:?}", resolver.available_events());
            let id = *id;
            println!("{}", id);
            sequence.push(id);
            resolver.pop_event(id);
            if resolver.available_events().len() == 9 {
                assert!(id == 109);
                break;
            }
            assert!(resolver.available_events().len() <= 2);
        }
    }
}
