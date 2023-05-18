use std::cmp::min;
use std::collections::hash_map::DefaultHasher;
use std::collections::{HashMap, HashSet};
use std::env;
use std::hash::{Hash, Hasher};
use std::io::Write;

use assertables::{assume, assume_eq};
use clap::Parser;
use dslab_mp::mc::events::McEvent;
use dslab_mp::mc::model_checker::ModelChecker;
use dslab_mp::mc::strategies::dfs::Dfs;
use dslab_mp::mc::strategies::random_walk::RandomWalk;
use dslab_mp::mc::strategy::McResult;
use dslab_mp::mc::system::McState;
use env_logger::Builder;
use log::LevelFilter;
use rand::prelude::*;
use rand_pcg::Pcg64;
use serde::{Deserialize, Serialize};
use sugars::boxed;

use dslab_mp::message::Message;
use dslab_mp::system::System;
use dslab_mp::test::{TestResult, TestSuite};
use dslab_mp_python::PyProcessFactory;

// UTILS -------------------------------------------------------------------------------------------

#[derive(Serialize)]
struct JoinMessage<'a> {
    seed: &'a str,
}

#[derive(Serialize)]
struct LeaveMessage {}

#[derive(Serialize)]
struct GetMembersMessage {}

#[derive(Deserialize)]
struct MembersMessage {
    members: Vec<String>,
}

#[derive(Clone)]
struct TestConfig<'a> {
    process_factory: &'a PyProcessFactory,
    process_count: u32,
    seed: u64,
}

fn init_logger(level: LevelFilter) {
    Builder::new()
        .filter(Some("dslab_mp"), level)
        .format(|buf, record| writeln!(buf, "{}", record.args()))
        .init();
}

fn build_system(config: &TestConfig) -> System {
    let mut sys = System::new(config.seed);
    sys.network().set_delays(0.01, 0.1);
    for n in 0..config.process_count {
        // process and node on which it runs have the same name
        let name = format!("{}", &n);
        sys.add_node(&name);
        let clock_skew = sys.gen_range(0.0..10.0);
        sys.set_node_clock_skew(&name, clock_skew);
        let process = config.process_factory.build((&name,), config.seed);
        sys.add_process(&name, boxed!(process), &name);
    }
    sys
}

fn initialize_group(sys: &mut System, group: &Vec<String>, seed: &str) -> TestResult {
    for proc in group {
        sys.send_local_message(proc, Message::json("JOIN", &JoinMessage { seed }));
    }
    step_until_stabilized(sys, group.clone().into_iter().collect())
}

fn crash_process(name: &str, sys: &mut System) {
    // we just crash the node on which the process is running
    sys.crash_node(name);
}

fn recover_process(name: &str, sys: &mut System, config: &TestConfig) {
    sys.recover_node(name);
    let process = config.process_factory.build((name,), config.seed);
    sys.add_process(name, boxed!(process), name);
}

fn step_until_stabilized(sys: &mut System, group: HashSet<String>) -> TestResult {
    let max_time = sys.time() + 300.; // timeout is 5 minutes
    let mut stabilized = HashSet::new();
    let mut memberlists = HashMap::new();

    while stabilized.len() < group.len() && sys.time() < max_time {
        let cont = sys.step_for_duration(5.);
        stabilized.clear();
        for proc in group.iter() {
            sys.send_local_message(proc, Message::json("GET_MEMBERS", &GetMembersMessage {}));
            let res = sys.step_until_local_message_timeout(proc, 10.);
            assume!(res.is_ok(), format!("Members list is not returned by {}", &proc))?;
            let msgs = res.unwrap();
            let msg = msgs.first().unwrap();
            assume!(msg.tip == "MEMBERS", "Wrong message type")?;
            let data: MembersMessage = serde_json::from_str(&msg.data).unwrap();
            let members: HashSet<String> = data.members.clone().into_iter().collect();
            if members.eq(&group) {
                stabilized.insert(proc.clone());
            }
            memberlists.insert(proc.clone(), data.members);
        }
        if !cont {
            break;
        }
    }

    if stabilized != group && group.len() <= 10 {
        println!("Members lists:");
        for proc in sys.process_names() {
            if group.contains(&proc) {
                let members = memberlists.get_mut(&proc).unwrap();
                members.sort();
                println!("- [{}] {}", proc, members.join(", "));
            }
        }
        let mut expected = group.clone().into_iter().collect::<Vec<_>>();
        expected.sort();
        println!("Expected group: {}", expected.join(", "));
    }
    assume_eq!(stabilized, group, "Group members lists are not stabilized")?;
    Ok(true)
}

// TESTS -------------------------------------------------------------------------------------------

fn test_simple(config: &TestConfig) -> TestResult {
    let mut sys = build_system(config);
    let group = sys.process_names();
    let seed = &group[0];
    initialize_group(&mut sys, &group, seed)
}

fn test_random_seed(config: &TestConfig) -> TestResult {
    let mut rand = Pcg64::seed_from_u64(config.seed);
    let mut sys = build_system(config);
    let mut group = Vec::new();
    for proc in sys.process_names() {
        let seed = match group.len() {
            0 => &proc,
            _ => group.choose(&mut rand).unwrap(),
        };
        sys.send_local_message(&proc, Message::json("JOIN", &JoinMessage { seed }));
        group.push(proc);
    }
    step_until_stabilized(&mut sys, group.into_iter().collect())
}

fn test_process_join(config: &TestConfig) -> TestResult {
    let mut rand = Pcg64::seed_from_u64(config.seed);
    let mut sys = build_system(config);
    let mut group = sys.process_names();
    group.shuffle(&mut rand);
    let new_proc = group.remove(rand.gen_range(0..group.len()));
    let seed = &group[0];
    initialize_group(&mut sys, &group, seed)?;

    // process joins the system
    sys.send_local_message(&new_proc, Message::json("JOIN", &JoinMessage { seed }));
    group.push(new_proc);
    step_until_stabilized(&mut sys, group.into_iter().collect())
}

fn test_process_leave(config: &TestConfig) -> TestResult {
    let mut rand = Pcg64::seed_from_u64(config.seed);
    let mut sys = build_system(config);
    let mut group = sys.process_names();
    group.shuffle(&mut rand);
    let seed = &group[0];
    initialize_group(&mut sys, &group, seed)?;

    // process leaves the system
    let left_proc = group.remove(rand.gen_range(0..group.len()));
    sys.send_local_message(&left_proc, Message::json("LEAVE", &LeaveMessage {}));
    step_until_stabilized(&mut sys, group.into_iter().collect())
}

fn test_process_crash(config: &TestConfig) -> TestResult {
    let mut rand = Pcg64::seed_from_u64(config.seed);
    let mut sys = build_system(config);
    let mut group = sys.process_names();
    group.shuffle(&mut rand);
    let seed = &group[0];
    initialize_group(&mut sys, &group, seed)?;

    // process crashes
    let crashed = group.remove(rand.gen_range(0..group.len()));
    crash_process(&crashed, &mut sys);
    step_until_stabilized(&mut sys, group.into_iter().collect())
}

fn test_seed_process_crash(config: &TestConfig) -> TestResult {
    let mut rand = Pcg64::seed_from_u64(config.seed);
    let mut sys = build_system(config);
    let mut group = sys.process_names();
    group.shuffle(&mut rand);
    let seed = &group[0].clone();
    initialize_group(&mut sys, &group, seed)?;

    // seed process crashes
    group.remove(0);
    crash_process(seed, &mut sys);
    step_until_stabilized(&mut sys, group.into_iter().collect())
}

fn test_process_crash_recover(config: &TestConfig) -> TestResult {
    let mut rand = Pcg64::seed_from_u64(config.seed);
    let mut sys = build_system(config);
    let mut group = sys.process_names();
    group.shuffle(&mut rand);
    let seed = &group[0].clone();
    initialize_group(&mut sys, &group, seed)?;

    // process crashes
    let crashed = group.remove(rand.gen_range(0..group.len()));
    crash_process(&crashed, &mut sys);
    step_until_stabilized(&mut sys, group.clone().into_iter().collect())?;

    // process recovers
    recover_process(&crashed, &mut sys, config);
    sys.send_local_message(&crashed, Message::json("JOIN", &JoinMessage { seed }));

    group.push(crashed);
    step_until_stabilized(&mut sys, group.into_iter().collect())
}

fn test_process_offline(config: &TestConfig) -> TestResult {
    let mut rand = Pcg64::seed_from_u64(config.seed);
    let mut sys = build_system(config);
    let mut group = sys.process_names();
    group.shuffle(&mut rand);
    let seed = &group[0];
    initialize_group(&mut sys, &group, seed)?;

    // process goes offline
    let offline_proc = group.remove(rand.gen_range(0..group.len()));
    sys.network().disconnect_node(&offline_proc);
    step_until_stabilized(&mut sys, group.into_iter().collect())
}

fn test_seed_process_offline(config: &TestConfig) -> TestResult {
    let mut rand = Pcg64::seed_from_u64(config.seed);
    let mut sys = build_system(config);
    let mut group = sys.process_names();
    group.shuffle(&mut rand);
    let seed = &group[0].clone();
    initialize_group(&mut sys, &group, seed)?;

    // seed process goes offline
    group.remove(0);
    sys.network().disconnect_node(seed);
    step_until_stabilized(&mut sys, group.into_iter().collect())
}

fn test_process_offline_recover(config: &TestConfig) -> TestResult {
    let mut rand = Pcg64::seed_from_u64(config.seed);
    let mut sys = build_system(config);
    let mut group = sys.process_names();
    group.shuffle(&mut rand);
    let seed = &group[0];
    initialize_group(&mut sys, &group, seed)?;

    // process goes offline
    let offline_proc = group.remove(rand.gen_range(0..group.len()));
    sys.network().disconnect_node(&offline_proc);
    step_until_stabilized(&mut sys, group.clone().into_iter().collect())?;

    // process goes back online
    sys.network().connect_node(&offline_proc);
    group.push(offline_proc);
    step_until_stabilized(&mut sys, group.into_iter().collect())
}

fn test_network_partition(config: &TestConfig) -> TestResult {
    let mut rand = Pcg64::seed_from_u64(config.seed);
    let mut sys = build_system(config);
    let mut group = sys.process_names();
    group.shuffle(&mut rand);
    let seed = &group[0];
    initialize_group(&mut sys, &group, seed)?;

    // network is partitioned
    let (group1, group2): (Vec<_>, Vec<_>) = group.iter().map(|s| &**s).partition(|_| rand.gen_range(0.0..1.0) > 0.6);
    sys.network().make_partition(&group1, &group2);
    step_until_stabilized(&mut sys, group1.into_iter().map(String::from).collect())?;
    step_until_stabilized(&mut sys, group2.into_iter().map(String::from).collect())
}

fn test_network_partition_recover(config: &TestConfig) -> TestResult {
    let mut rand = Pcg64::seed_from_u64(config.seed);
    let mut sys = build_system(config);
    let mut group = sys.process_names();
    group.shuffle(&mut rand);
    let seed = &group[0];
    initialize_group(&mut sys, &group, seed)?;

    // network is partitioned
    let (group1, group2): (Vec<_>, Vec<_>) = group.iter().map(|s| &**s).partition(|_| rand.gen_range(0.0..1.0) > 0.6);
    sys.network().make_partition(&group1, &group2);
    step_until_stabilized(&mut sys, group1.into_iter().map(String::from).collect())?;
    step_until_stabilized(&mut sys, group2.into_iter().map(String::from).collect())?;

    // network is recovered
    sys.network().reset_network();
    step_until_stabilized(&mut sys, group.into_iter().map(String::from).collect())
}

fn test_process_cannot_receive(config: &TestConfig) -> TestResult {
    let mut rand = Pcg64::seed_from_u64(config.seed);
    let mut sys = build_system(config);
    let mut group = sys.process_names();
    group.shuffle(&mut rand);
    let seed = &group[0];
    initialize_group(&mut sys, &group, seed)?;

    // process goes partially offline (cannot receive incoming messages)
    let blocked_proc = group.remove(rand.gen_range(0..group.len()));
    sys.network().drop_incoming(&blocked_proc);
    step_until_stabilized(&mut sys, group.into_iter().collect())
}

fn test_process_cannot_send(config: &TestConfig) -> TestResult {
    let mut rand = Pcg64::seed_from_u64(config.seed);
    let mut sys = build_system(config);
    let mut group = sys.process_names();
    group.shuffle(&mut rand);
    let seed = &group[0];
    initialize_group(&mut sys, &group, seed)?;

    // process goes partially offline (cannot send outgoing messages)
    let blocked_proc = group.remove(rand.gen_range(0..group.len()));
    sys.network().drop_outgoing(&blocked_proc);
    step_until_stabilized(&mut sys, group.into_iter().collect())
}

fn test_two_processes_cannot_communicate(config: &TestConfig) -> TestResult {
    let mut rand = Pcg64::seed_from_u64(config.seed);
    let mut sys = build_system(config);
    let mut group = sys.process_names();
    group.shuffle(&mut rand);
    let seed = &group[0].clone();
    initialize_group(&mut sys, &group, seed)?;

    // two processes cannot communicate with each other
    let proc1 = seed;
    let proc2 = group.get(rand.gen_range(1..group.len())).unwrap();
    sys.network().disable_link(proc1, proc2);
    sys.network().disable_link(proc2, proc1);
    // run for a while
    sys.steps(1000);
    step_until_stabilized(&mut sys, group.into_iter().collect())
}

fn test_slow_network(config: &TestConfig) -> TestResult {
    let mut rand = Pcg64::seed_from_u64(config.seed);
    let mut sys = build_system(config);
    let mut group = sys.process_names();
    group.shuffle(&mut rand);
    let seed = &group[0];
    initialize_group(&mut sys, &group, seed)?;

    // slow down network for a while
    sys.network().set_delays(0.1, 1.0);
    sys.steps(200);
    sys.network().set_delays(0.01, 0.1);
    step_until_stabilized(&mut sys, group.into_iter().collect())
}

fn test_flaky_network(config: &TestConfig) -> TestResult {
    let mut rand = Pcg64::seed_from_u64(config.seed);
    let mut sys = build_system(config);
    let mut group = sys.process_names();
    group.shuffle(&mut rand);
    let seed = &group[0];
    initialize_group(&mut sys, &group, seed)?;

    // make network unreliable for a while
    sys.network().set_drop_rate(0.5);
    sys.steps(1000);
    sys.network().set_drop_rate(0.0);
    step_until_stabilized(&mut sys, group.into_iter().collect())
}

fn test_flaky_network_on_start(config: &TestConfig) -> TestResult {
    let mut rand = Pcg64::seed_from_u64(config.seed);
    let mut sys = build_system(config);
    let mut group = sys.process_names();
    group.shuffle(&mut rand);
    let seed = &group[0];

    // make network unreliable from the start
    sys.network().set_drop_rate(0.2);
    for proc in &group {
        sys.send_local_message(proc, Message::json("JOIN", &JoinMessage { seed }));
    }
    sys.steps(1000);
    sys.network().set_drop_rate(0.0);
    step_until_stabilized(&mut sys, group.into_iter().collect())
}

fn test_flaky_network_and_crash(config: &TestConfig) -> TestResult {
    let mut rand = Pcg64::seed_from_u64(config.seed);
    let mut sys = build_system(config);
    let mut group = sys.process_names();
    group.shuffle(&mut rand);
    let seed = &group[0];
    initialize_group(&mut sys, &group, seed)?;

    // make network unreliable for a while + crash process
    sys.network().set_drop_rate(0.5);
    let crashed = group.remove(rand.gen_range(0..group.len()));
    crash_process(&crashed, &mut sys);
    sys.steps(1000);
    sys.network().set_drop_rate(0.0);
    step_until_stabilized(&mut sys, group.into_iter().collect())
}

fn test_chaos_monkey(config: &TestConfig) -> TestResult {
    let mut rand = Pcg64::seed_from_u64(config.seed);
    let mut sys = build_system(config);
    let mut group = sys.process_names();
    group.shuffle(&mut rand);
    let seed = &group[0];
    initialize_group(&mut sys, &group, seed)?;

    for _ in 0..5 {
        let p = rand.gen_range(0.0..1.0);
        // do some nasty things
        match p {
            p if p < 0.25 => {
                // crash process
                let victim = group.remove(rand.gen_range(0..group.len()));
                crash_process(&victim, &mut sys);
            }
            p if p < 0.5 => {
                // disconnect process
                let victim = group.remove(rand.gen_range(0..group.len()));
                sys.network().disconnect_node(&victim);
            }
            p if p < 0.75 => {
                // partially disconnect process (cannot receive)
                let victim = group.remove(rand.gen_range(0..group.len()));
                sys.network().drop_incoming(&victim);
            }
            _ => {
                // two processes cannot communicate with each other
                let proc1 = group.get(rand.gen_range(0..group.len())).unwrap();
                let mut proc2 = group.get(rand.gen_range(0..group.len())).unwrap();
                while proc1 == proc2 {
                    proc2 = group.get(rand.gen_range(0..group.len())).unwrap();
                }
                sys.network().disable_link(proc1, proc2);
                sys.network().disable_link(proc2, proc1);
            }
        }
        step_until_stabilized(&mut sys, group.clone().into_iter().collect())?;
    }
    Ok(true)
}

fn test_scalability_normal(config: &TestConfig) -> TestResult {
    let sys_sizes = [
        config.process_count,
        config.process_count * 2,
        config.process_count * 5,
        config.process_count * 10,
    ];
    let mut measurements = Vec::new();
    for size in sys_sizes {
        let mut run_config = config.clone();
        run_config.process_count = size;
        let mut rand = Pcg64::seed_from_u64(config.seed);
        let mut sys = build_system(&run_config);
        let mut group = sys.process_names();
        group.shuffle(&mut rand);
        let seed = &group[0];
        initialize_group(&mut sys, &group, seed)?;

        let init_time = sys.time();
        let init_net_traffic = sys.network().traffic();
        let init_msg_count = sys.network().message_count();
        let mut init_loads = HashMap::new();
        for proc in sys.process_names() {
            init_loads.insert(
                proc.clone(),
                sys.sent_message_count(&proc) + sys.received_message_count(&proc),
            );
        }

        sys.step_for_duration(10.0);

        let mut loads = Vec::new();
        for proc in sys.process_names() {
            let load = sys.sent_message_count(&proc) + sys.received_message_count(&proc);
            loads.push(load - init_loads.get(&proc).unwrap());
        }
        let min_load = *loads.iter().min().unwrap();
        let max_load = *loads.iter().max().unwrap();
        let duration = sys.time() - init_time;
        let traffic = sys.network().traffic();
        let message_count = sys.network().message_count();
        measurements.push((
            duration,
            (traffic - init_net_traffic) as f64 / duration,
            (message_count - init_msg_count) as f64 / duration,
            max_load as f64 / duration,
            max_load as f64 / min_load as f64,
        ));
    }
    let mut scaling_ok = true;
    let mut load_ratio_ok = true;
    for i in 0..sys_sizes.len() {
        let (time, traffic, message_count, max_load, load_ratio) = measurements[i];
        println!(
            "- N = {}: time - {:.2}, traffic/s - {:.2}, messages/s - {:.2}, max load - {:.2}, max/min load - {:.2}",
            sys_sizes[i], time, traffic, message_count, max_load, load_ratio
        );
        if load_ratio > 5.0 {
            load_ratio_ok = false;
        }
        if i > 0 {
            let size_ratio = sys_sizes[i] as f64 / sys_sizes[i - 1] as f64;
            let traffic_ratio = traffic / measurements[i - 1].1;
            let messages_ratio = message_count / measurements[i - 1].2;
            if traffic_ratio > 2.0 * size_ratio || messages_ratio > 2.0 * size_ratio {
                scaling_ok = false;
            }
        }
    }
    assume!(scaling_ok, "Bad network load scaling")?;
    assume!(load_ratio_ok, "Bad max/min process load")?;
    Ok(true)
}

fn test_scalability_crash(config: &TestConfig) -> TestResult {
    let sys_sizes = [
        config.process_count,
        config.process_count * 2,
        config.process_count * 5,
        config.process_count * 10,
    ];
    let mut measurements = Vec::new();
    for size in sys_sizes {
        let mut run_config = config.clone();
        run_config.process_count = size;
        let mut rand = Pcg64::seed_from_u64(config.seed);
        let mut sys = build_system(&run_config);
        let mut group = sys.process_names();
        group.shuffle(&mut rand);
        let seed = &group[0];
        initialize_group(&mut sys, &group, seed)?;

        let init_time = sys.time();
        let init_net_traffic = sys.network().traffic();
        let init_msg_count = sys.network().message_count();
        let mut init_loads = HashMap::new();
        for proc in sys.process_names() {
            init_loads.insert(
                proc.clone(),
                sys.sent_message_count(&proc) + sys.received_message_count(&proc),
            );
        }

        let crashed = group.remove(rand.gen_range(0..group.len()));
        crash_process(&crashed, &mut sys);
        step_until_stabilized(&mut sys, group.clone().into_iter().collect())?;

        let mut loads = Vec::new();
        for proc in sys.process_names() {
            if proc != crashed {
                let load = sys.sent_message_count(&proc) + sys.received_message_count(&proc);
                loads.push(load - init_loads.get(&proc).unwrap());
            }
        }
        let min_load = *loads.iter().min().unwrap();
        let max_load = *loads.iter().max().unwrap();
        let duration = sys.time() - init_time;
        let traffic = sys.network().traffic();
        let message_count = sys.network().message_count();
        measurements.push((
            duration,
            (traffic - init_net_traffic) as f64 / duration,
            (message_count - init_msg_count) as f64 / duration,
            max_load as f64 / duration,
            max_load as f64 / min_load as f64,
        ));
    }
    let mut scaling_ok = true;
    let mut load_ratio_ok = true;
    for i in 0..sys_sizes.len() {
        let (time, traffic, message_count, max_load, load_ratio) = measurements[i];
        println!(
            "- N = {}: time - {:.2}, traffic/s - {:.2}, messages/s - {:.2}, max load - {:.2}, max/min load - {:.2}",
            sys_sizes[i], time, traffic, message_count, max_load, load_ratio
        );
        if load_ratio > 5.0 {
            load_ratio_ok = false;
        }
        if i > 0 {
            let size_ratio = sys_sizes[i] as f64 / sys_sizes[i - 1] as f64;
            let traffic_ratio = traffic / measurements[i - 1].1;
            let messages_ratio = message_count / measurements[i - 1].2;
            if traffic_ratio > 2.0 * size_ratio || messages_ratio > 2.0 * size_ratio {
                scaling_ok = false;
            }
        }
    }
    assume!(scaling_ok, "Bad network load scaling")?;
    assume!(load_ratio_ok, "Bad max/min process load")?;
    Ok(true)
}

fn mc_clear_state_history(state: &mut McState) {
    state.log.clear();
    for (node, node_state) in state.node_states.iter_mut() {
        node_state.get_mut(node).map(|y| y.sent_message_count = 0);
        node_state.get_mut(node).map(|y| y.received_message_count = 0);
        node_state.get_mut(node).map(|y| y.local_outbox.clear());
        node_state.get_mut(node).map(|y| y.event_log.clear());
    }
}

fn mc_prune_none<'a>() -> Box<dyn Fn(&McState) -> Option<String> + 'a> {
    Box::new(|_| None)
}

fn mc_invariant_ok<'a>() -> Box<dyn Fn(&McState) -> Result<(), String> + 'a> {
    Box::new(|_| Ok(()))
}

fn mc_collect_responses<'a>(
    procs: &'a Vec<String>,
    groups: &'a Vec<&HashSet<String>>,
) -> Box<dyn Fn(&McState) -> bool + 'a> {
    Box::new(|state| {
        let mut correct_answers = 0;
        for node in state.node_states.keys() {
            if let Some(msg) = state.node_states[node][node].local_outbox.iter().next() {
                let data: MembersMessage = serde_json::from_str(&msg.data).unwrap();
                let members = HashSet::from_iter(data.members.into_iter());
                let ground_truth = groups.iter().find(|g| g.contains(node)).unwrap();
                let mut correct = (*ground_truth).difference(&members).any(|_| true) == false;
                correct &= members.difference(&ground_truth).any(|_| true) == false;
                if correct {
                    correct_answers += 1;
                }
            }
        }
        correct_answers == procs.len()
    })
}

fn mc_invariant_depth(state: &McState, max_depth: u64, err_msg: &str) -> Result<(), String> {
    if state.search_depth > max_depth {
        Err(err_msg.to_owned())
    } else {
        Ok(())
    }
}

fn mc_invariant_check_response_type(state: &McState) -> Result<(), String> {
    for node in state.node_states.keys() {
        if let Some(msg) = state.node_states[node][node].local_outbox.iter().next() {
            if msg.tip != "MEMBERS" {
                return Err("wrong message type".to_owned());
            }
        }
    }
    Ok(())
}

fn mc_invariant_check_stabilized(state: &McState, groups: &Vec<&HashSet<String>>) -> Result<(), String> {
    for node in state.node_states.keys() {
        if let Some(msg) = state.node_states[node][node].local_outbox.iter().next() {
            let data: MembersMessage = serde_json::from_str(&msg.data).unwrap();
            let members = HashSet::from_iter(data.members.into_iter());
            let ground_truth = groups.iter().find(|g| g.contains(node)).unwrap();
            let mut correct = (*ground_truth).difference(&members).any(|_| true) == false;
            correct &= members.difference(&ground_truth).any(|_| true) == false;
            if !correct {
                return Err("still not stabilized".to_owned());
            }
        }
    }
    Ok(())
}

fn mc_prune_exploration<'a>() -> Box<dyn Fn(&McState) -> Option<String> + 'a> {
    Box::new(move |state| {
        mc_prune_messages(state, 5 as usize, 8 as usize)
            .or_else(|| mc_prune_timers(state, 10 as usize, 18 as usize).or_else(|| mc_prune_repetitions(state, 3)))
    })
}

fn mc_prune_timers(state: &McState, for_a_proc: usize, total: usize) -> Option<String> {
    let mut counter: HashMap<String, usize> = HashMap::new();
    for event in &state.log {
        if let McEvent::TimerFired { proc, .. } = event {
            *counter.entry(proc.clone()).or_insert(0) += 1;
        }
    }
    for (proc, cnt) in counter {
        if cnt > for_a_proc {
            return Some(format!("too many timers for proc {}", proc).to_owned());
        }
    }

    if state
        .log
        .iter()
        .filter(|event| match *event {
            McEvent::TimerFired { .. } => true,
            _ => false,
        })
        .count()
        > total
    {
        return Some("too many timers in total".to_owned());
    }
    None
}

fn mc_prune_repetitions(state: &McState, max_repetition_size: u64) -> Option<String> {
    let mut counter: HashMap<McEvent, u64> = HashMap::new();
    for event in state.log.clone() {
        let entry = counter.entry(event).or_default();
        *entry += 1;
        if *entry > max_repetition_size {
            return Some("too many repetitions for an event".to_owned());
        }
    }
    None
}

fn mc_prune_messages(state: &McState, for_a_proc: usize, total: usize) -> Option<String> {
    let mut counter: HashMap<String, usize> = HashMap::new();
    for event in &state.log {
        if let McEvent::MessageReceived { src, .. } = event {
            *counter.entry(src.clone()).or_insert(0) += 1;
        }
        if let McEvent::MessageDropped { src, .. } = event {
            *counter.entry(src.clone()).or_insert(0) += 1;
        }
    }
    for (proc, cnt) in counter {
        if cnt > for_a_proc {
            return Some(format!("too many messages for proc {}", proc).to_owned());
        }
    }

    if state
        .log
        .iter()
        .filter(|event| match *event {
            McEvent::MessageReceived { .. } => true,
            McEvent::MessageDropped { .. } => true,
            _ => false,
        })
        .count()
        > total
    {
        return Some("too many messages in total".to_owned());
    }
    None
}

fn mc_goal_get_response<'a>(node: &'a str) -> Box<dyn Fn(&McState) -> Option<String> + 'a> {
    Box::new(|state| {
        if !state.node_states[node][node].local_outbox.is_empty() {
            Some("got_result".to_owned())
        } else {
            None
        }
    })
}

fn mc_goal_get_responses<'a>(
    procs: &'a Vec<String>,
    groups: &'a Vec<&HashSet<String>>,
) -> Box<dyn Fn(&McState) -> Option<String> + 'a> {
    Box::new(|state| {
        let mut correct_answers = 0;
        for node in state.node_states.keys() {
            if let Some(msg) = state.node_states[node][node].local_outbox.iter().next() {
                let data: MembersMessage = serde_json::from_str(&msg.data).unwrap();
                let members = HashSet::from_iter(data.members.into_iter());
                let ground_truth = groups.iter().find(|g| g.contains(node)).unwrap();
                let correct = (*ground_truth).difference(&members).any(|_| true) == false;
                if correct {
                    correct_answers += 1;
                }
            }
        }
        if correct_answers == procs.len() {
            Some("all responses are correct".to_owned())
        } else {
            None
        }
    })
}

fn get_n_start_states(start_states: HashSet<McState>, mut n: usize) -> HashSet<McState> {
    n = min(n, start_states.len());
    let mut hashed = start_states
        .into_iter()
        .map(|state| {
            let mut hasher = DefaultHasher::default();
            state.hash(&mut hasher);
            (hasher.finish(), state)
        })
        .collect::<Vec<(u64, McState)>>();
    hashed.sort_by_key(|(a, b)| *a);
    HashSet::from_iter(hashed.split_at(n).0.into_iter().map(|(a, b)| b.clone()))
}

fn mc_stabilize(
    sys: &mut System,
    groups: Vec<&HashSet<String>>,
    start_states: Option<HashSet<McState>>,
) -> Result<McResult, String> {
    let procs = sys.process_names();
    let old_delivery_delay = sys.network().max_delay();
    sys.network().set_delay(0.0);
    let mut mc = ModelChecker::new(
        &sys,
        Box::new(Dfs::new(
            Box::new(|state| {
                mc_prune_timers(state, 5, 10).or_else(|| {
                    mc_invariant_depth(state, 15, "too deep")
                        .err()
                        .or_else(|| mc_prune_repetitions(state, 2))
                })
            }),
            Box::new(|state| {
                mc_invariant_depth(state, 20, "enough steps").err().or_else(|| {
                    if state.events.available_events_num() == 0 {
                        Some("nothing left to do".to_owned())
                    } else {
                        None
                    }
                })
            }),
            Box::new(mc_invariant_ok()),
            Some(Box::new(|state| {
                if procs
                    .iter()
                    .all(|proc| state.node_states[proc][proc].received_message_count > 2)
                {
                    return true;
                }
                mc_invariant_depth(state, 15, "too deep")
                    .err()
                    .or_else(|| {
                        if state.events.available_events_num() == 0 {
                            Some("nothing left to do".to_owned())
                        } else {
                            None
                        }
                    })
                    .is_some()
            })),
            dslab_mp::mc::strategy::ExecutionMode::Debug,
        )),
    );
    let mut res = McResult::default();
    if let Some(mut start_states) = start_states {
        start_states = get_n_start_states(start_states, 1);
        for mut start_state in start_states {
            mc_clear_state_history(&mut start_state);
            start_state.events.is_insta = true;
            mc.set_state(start_state);
            let cur_res = mc.run();
            if cur_res.is_err() {
                return Err(format!(
                    "model checher found error: {}",
                    cur_res.as_ref().err().unwrap()
                ));
            }
            let cur_res = cur_res.unwrap();
            res.combine(cur_res);
        }
    } else {
        let cur_res = mc.run();
        if cur_res.is_err() {
            return Err(format!(
                "model checher found error: {}",
                cur_res.as_ref().err().unwrap()
            ));
        }
        res = cur_res.unwrap();
    }
    println!("got {} states", res.clone().collected.len());
    println!("{:?}", res.clone().summary);
    sys.network().set_delay(old_delivery_delay);
    let res = mc_collect_members(sys, res.collected, &procs, &groups)?;
    if res.collected.is_empty() {
        return Err("not stabilized".to_owned());
    }
    Ok(res)
}

fn mc_collect_members(
    sys: &mut System,
    mut collected: HashSet<McState>,
    procs: &Vec<String>,
    groups: &Vec<&HashSet<String>>,
) -> Result<McResult, String> {
    collected = get_n_start_states(collected, 20);
    let mut mc = ModelChecker::new(
        &sys,
        Box::new(Dfs::new(
            mc_prune_none(),
            mc_goal_get_responses(procs, groups),
            Box::new(|state| {
                mc_invariant_check_response_type(state)?;
                mc_invariant_check_stabilized(state, groups)?;
                mc_invariant_depth(state, 2, "no need to do extra steps")
            }),
            Some(mc_collect_responses(procs, groups)),
            dslab_mp::mc::strategy::ExecutionMode::Debug,
        )),
    );
    let mut combined = McResult::default();
    for state in collected {
        mc.set_state(state);
        for node in procs.iter() {
            mc.apply_event(dslab_mp::mc::events::McEvent::LocalMessageReceived {
                msg: Message::json("GET_MEMBERS", &GetMembersMessage {}),
                dest: node.clone(),
            });
        }
        let res = mc.run();
        if res.is_err() {
            return Err(format!("model checher found error: {}", res.as_ref().err().unwrap()));
        }
        combined.combine(res.unwrap());
    }
    println!("{:?}", combined.summary);
    Ok(combined)
}

fn mc_explore(
    sys: &mut System,
    groups: Vec<&HashSet<String>>,
    start_states: Option<HashSet<McState>>,
) -> Result<McResult, String> {
    let procs = sys.process_names();

    // generally insta_mode would be bad but we know messages will be dropped
    let old_delivery_delay = sys.network().max_delay();
    sys.network().set_delay(0.0);
    let mut mc = ModelChecker::new(
        &sys,
        Box::new(RandomWalk::new(
            mc_prune_exploration(),
            Box::new(|state| {
                mc_invariant_depth(state, 1030, "enough steps").err().or_else(|| {
                    if state.events.available_events_num() == 0 {
                        Some("finished".to_owned())
                    } else {
                        None
                    }
                })
            }),
            Box::new(mc_invariant_ok()),
            Some(Box::new(|state| {
                mc_invariant_depth(state, 1030, "enough steps").is_err() || state.events.available_events_num() == 0
            })),
            dslab_mp::mc::strategy::ExecutionMode::Debug,
            5000,
            1020,
        )),
    );
    let mut res = McResult::default();
    if let Some(start_states) = start_states {
        for mut start_state in start_states {
            mc_clear_state_history(&mut start_state);
            for event_id in start_state.events.available_events() {
                if let Some(McEvent::MessageReceived { .. }) = start_state.events.get(event_id) {
                    start_state.events.pop(event_id);
                }
                if let Some(McEvent::MessageDropped { .. }) = start_state.events.get(event_id) {
                    start_state.events.pop(event_id);
                }
            }
            start_state.search_depth = 1000;
            mc.set_state_same_depth(start_state);
            let cur_res = mc.run();
            if cur_res.is_err() {
                return Err(format!(
                    "model checher found error: {}",
                    cur_res.as_ref().err().unwrap()
                ));
            }
            let cur_res = cur_res.unwrap();
            res.combine(cur_res);
        }
    } else {
        let cur_res = mc.run();
        if cur_res.is_err() {
            return Err(format!(
                "model checher found error: {}",
                cur_res.as_ref().err().unwrap()
            ));
        }
        res = cur_res.unwrap();
    }
    println!("got {} states", res.clone().collected.len());
    println!("{:?}", res.clone().summary);
    sys.network().set_delay(old_delivery_delay);
    let res = mc_collect_members(sys, res.collected, &procs, &groups)?;
    if res.collected.is_empty() {
        return Err("not stabilized".to_owned());
    }
    Ok(res)
}

fn test_mc_local_answer(config: &TestConfig) -> TestResult {
    let mut sys = build_system(config);
    let group = sys.process_names();
    sys.send_local_message(&group[0], Message::json("JOIN", &JoinMessage { seed: &group[0] }));
    sys.send_local_message(&group[0], Message::json("GET_MEMBERS", &GetMembersMessage {}));
    let mut mc = ModelChecker::new(
        &sys,
        Box::new(Dfs::new(
            mc_prune_none(),
            mc_goal_get_response(&group[0]),
            Box::new(move |state| mc_invariant_depth(state, 5, "too_deep")),
            None,
            dslab_mp::mc::strategy::ExecutionMode::Debug,
        )),
    );
    let res = mc.run();
    if res.is_err() {
        return Err(format!("model checher found error: {}", res.as_ref().err().unwrap()));
    }
    println!("{:?}", res.unwrap());
    Ok(true)
}

fn test_mc_group(config: &TestConfig) -> TestResult {
    let mut rand = Pcg64::seed_from_u64(config.seed);
    let mut sys = build_system(config);
    let mut group = sys.process_names();
    group.shuffle(&mut rand);
    let seed = &group[0];
    for proc in &group {
        sys.send_local_message(proc, Message::json("JOIN", &JoinMessage { seed }));
    }

    mc_stabilize(&mut sys, vec![&HashSet::from_iter(group.into_iter())], None)?;
    Ok(true)
}

fn test_mc_partition(config: &TestConfig) -> TestResult {
    let mut rand = Pcg64::seed_from_u64(config.seed);
    let mut sys = build_system(config);
    let mut group = sys.process_names();
    group.shuffle(&mut rand);
    let seed = &group[0];
    for proc in &group {
        sys.send_local_message(proc, Message::json("JOIN", &JoinMessage { seed }));
    }

    let states_group = mc_stabilize(&mut sys, vec![&HashSet::from_iter(group.clone().into_iter())], None)?;
    sys.network().make_partition(&[&group[0]], &[&group[1]]);
    let start_states = get_n_start_states(states_group.collected, 1);
    let states_partition = mc_explore(
        &mut sys,
        vec![
            &HashSet::from_iter(vec![group[0].clone()].into_iter()),
            &HashSet::from_iter(vec![group[1].clone()].into_iter()),
        ],
        Some(start_states),
    )?;
    sys.network().reset_network();
    let start_states = get_n_start_states(states_partition.collected, 1);
    mc_stabilize(
        &mut sys,
        vec![&HashSet::from_iter(group.into_iter())],
        Some(start_states),
    )?;
    Ok(true)
}

// CLI -----------------------------------------------------------------------------------------------------------------

/// Membership Homework Tests
#[derive(Parser, Debug)]
#[clap(about, long_about = None)]
struct Args {
    /// Path to Python file with solution
    #[clap(long = "impl", short = 'i', default_value = "python/solution.py")]
    solution_path: String,

    /// Test to run (optional)
    #[clap(long = "test", short)]
    test: Option<String>,

    /// Print execution trace
    #[clap(long, short)]
    debug: bool,

    /// Random seed
    #[clap(long, short, default_value = "123")]
    seed: u64,

    /// Number of processes
    #[clap(long, short, default_value = "10")]
    process_count: u32,

    /// Number of chaos monkey runs
    #[clap(long, short, default_value = "100")]
    monkeys: u32,
}

// MAIN --------------------------------------------------------------------------------------------

fn main() {
    let args = Args::parse();
    if args.debug {
        init_logger(LevelFilter::Trace);
    }
    env::set_var("PYTHONPATH", "../../crates/dslab-mp-python/python");
    env::set_var("PYTHONHASHSEED", args.seed.to_string());
    let process_factory = PyProcessFactory::new(&args.solution_path, "GroupMember");
    let config = TestConfig {
        process_factory: &process_factory,
        process_count: args.process_count,
        seed: args.seed,
    };
    let mut tests = TestSuite::new();

    tests.add("SIMPLE", test_simple, config.clone());
    tests.add("RANDOM SEED", test_random_seed, config.clone());
    tests.add("PROCESS JOIN", test_process_join, config.clone());
    tests.add("PROCESS LEAVE", test_process_leave, config.clone());
    tests.add("PROCESS CRASH", test_process_crash, config.clone());
    tests.add("SEED PROCESS CRASH", test_seed_process_crash, config.clone());
    tests.add("PROCESS CRASH RECOVER", test_process_crash_recover, config.clone());
    tests.add("PROCESS OFFLINE", test_process_offline, config.clone());
    tests.add("SEED PROCESS OFFLINE", test_seed_process_offline, config.clone());
    tests.add("PROCESS OFFLINE RECOVER", test_process_offline_recover, config.clone());
    tests.add("PROCESS CANNOT RECEIVE", test_process_cannot_receive, config.clone());
    tests.add("PROCESS CANNOT SEND", test_process_cannot_send, config.clone());
    tests.add("NETWORK PARTITION", test_network_partition, config.clone());
    tests.add(
        "NETWORK PARTITION RECOVER",
        test_network_partition_recover,
        config.clone(),
    );
    tests.add(
        "TWO PROCESSES CANNOT COMMUNICATE",
        test_two_processes_cannot_communicate,
        config.clone(),
    );
    tests.add("SLOW NETWORK", test_slow_network, config.clone());
    tests.add("FLAKY NETWORK", test_flaky_network, config.clone());
    tests.add("FLAKY NETWORK ON START", test_flaky_network_on_start, config.clone());
    tests.add("FLAKY NETWORK AND CRASH", test_flaky_network_and_crash, config.clone());
    let mut rand = Pcg64::seed_from_u64(config.seed);
    for run in 1..=args.monkeys {
        let mut run_config = config.clone();
        run_config.seed = rand.next_u64();
        tests.add(&format!("CHAOS MONKEY (run {})", run), test_chaos_monkey, run_config);
    }
    tests.add("SCALABILITY NORMAL", test_scalability_normal, config.clone());
    tests.add("SCALABILITY CRASH", test_scalability_crash, config.clone());

    let mc_config = TestConfig {
        process_factory: &process_factory,
        process_count: 2,
        seed: args.seed,
    };
    tests.add("MC LOCAL GET_MEMBERS", test_mc_local_answer, mc_config.clone());
    tests.add("MC NORMAL", test_mc_group, mc_config.clone());
    // tests.add("MC PARTITION", test_mc_partition, mc_config.clone());

    if args.test.is_none() {
        tests.run();
    } else {
        tests.run_test(&args.test.unwrap());
    }
}
