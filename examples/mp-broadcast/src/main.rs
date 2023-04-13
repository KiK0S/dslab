use std::cmp::min;
use std::collections::hash_map::DefaultHasher;
use std::collections::{HashMap, HashSet};
use std::env;
use std::hash::{Hasher, Hash};
use std::io::Write;

use clap::Parser;
use dslab_mp::mc::events::McEvent;
use dslab_mp::mc::model_checker::ModelChecker;
use dslab_mp::mc::strategy::{McSummary, VisitedStates, McResult};
use dslab_mp::mc::system::McState;
use dslab_mp::mc::strategies::dfs::Dfs;
use env_logger::Builder;
use log::LevelFilter;
use rand::prelude::*;
use rand_pcg::Pcg64;
use serde_json::Value;
use sugars::boxed;

use dslab_mp::message::Message;
use dslab_mp::node::ProcessEvent;
use dslab_mp::system::System;
use dslab_mp::test::{TestResult, TestSuite};
use dslab_mp_python::PyProcessFactory;

// UTILS -------------------------------------------------------------------------------------------

#[derive(Copy, Clone)]
struct TestConfig<'a> {
    proc_factory: &'a PyProcessFactory,
    proc_count: u64,
    seed: u64,
    monkeys: u32,
    debug: bool,
}

fn init_logger(level: LevelFilter) {
    Builder::new()
        .filter(None, level)
        .format(|buf, record| writeln!(buf, "{}", record.args()))
        .init();
}

fn build_system(config: &TestConfig) -> System {
    let mut sys = System::new(config.seed);
    let mut proc_names = Vec::new();
    for n in 0..config.proc_count {
        proc_names.push(format!("proc-{}", n));
    }
    for n in 0..config.proc_count {
        let proc_name = &proc_names[n as usize];
        let proc = config.proc_factory.build((proc_name, proc_names.clone()), config.seed);
        let node_name = &format!("node-{}", n);
        sys.add_node(node_name);
        sys.add_process(proc_name, boxed!(proc), node_name);
    }
    sys
}

fn check(sys: &mut System, config: &TestConfig) -> TestResult {
    let mut sent = HashMap::new();
    let mut delivered = HashMap::new();
    let mut all_sent = HashSet::new();
    let mut all_delivered = HashSet::new();
    let mut histories = HashMap::new();
    for proc in 0..config.proc_count {
        let proc = format!("proc-{}", proc);
        let mut history = Vec::new();
        let mut sent_msgs = Vec::new();
        let mut delivered_msgs = Vec::new();
        for e in sys.event_log(&proc) {
            match e.event {
                ProcessEvent::LocalMessageReceived { msg: m } => {
                    let data: Value = serde_json::from_str(&m.data).unwrap();
                    let message = data["text"].as_str().unwrap().to_string();
                    sent_msgs.push(message.clone());
                    all_sent.insert(message.clone());
                    history.push(message);
                }
                ProcessEvent::LocalMessageSent { msg: m } => {
                    let data: Value = serde_json::from_str(&m.data).unwrap();
                    let message = data["text"].as_str().unwrap().to_string();
                    delivered_msgs.push(message.clone());
                    all_delivered.insert(message.clone());
                    history.push(message);
                }
                _ => {}
            }
        }
        sent.insert(proc.clone(), sent_msgs);
        delivered.insert(proc.clone(), delivered_msgs);
        histories.insert(proc, history);
    }
    if config.debug {
        println!("Messages sent across network: {}", sys.network().message_count());
        println!("Process histories:");
        for proc in sys.process_names() {
            println!("- [{}] {}", proc, histories.get(&proc).unwrap().join(", "));
        }
    }

    // NO DUPLICATION
    let mut no_duplication = true;
    for delivered_msgs in delivered.values() {
        let mut uniq = HashSet::new();
        for msg in delivered_msgs {
            if uniq.contains(msg) {
                println!("Message '{}' is duplicated!", msg);
                no_duplication = false;
            };
            uniq.insert(msg);
        }
    }

    // NO CREATION
    let mut no_creation = true;
    for delivered_msgs in delivered.values() {
        for msg in delivered_msgs {
            if !all_sent.contains(msg) {
                println!("Message '{}' was not sent!", msg);
                no_creation = false;
            }
        }
    }

    // VALIDITY
    let mut validity = true;
    for (proc, sent_msgs) in &sent {
        if sys.node_is_crashed(&sys.proc_node_name(proc)) {
            continue;
        }
        let delivered_msgs = delivered.get(proc).unwrap();
        for msg in sent_msgs {
            if !delivered_msgs.contains(msg) {
                println!("Process {} has not delivered its own message '{}'!", proc, msg);
                validity = false;
            }
        }
    }

    // UNIFORM AGREEMENT
    let mut uniform_agreement = true;
    for msg in all_delivered.iter() {
        for (proc, delivered_msgs) in &delivered {
            if sys.node_is_crashed(&sys.proc_node_name(proc)) {
                continue;
            }
            if !delivered_msgs.contains(msg) {
                println!("Message '{}' is not delivered by correct process {}!", msg, proc);
                uniform_agreement = false;
            }
        }
    }

    // CAUSAL ORDER
    let mut causal_order = true;
    for (src, sent_msgs) in &sent {
        for msg in sent_msgs.iter() {
            if !all_delivered.contains(msg) {
                continue;
            }
            // build sender past for send message event
            let mut src_past = HashSet::new();
            for e in histories.get(src).unwrap() {
                if e != msg {
                    src_past.insert(e.clone());
                } else {
                    break;
                }
            }
            // check that other correct nodes have delivered all past events before delivering the message
            for (dst, delivered_msgs) in &delivered {
                if sys.node_is_crashed(&sys.proc_node_name(dst)) {
                    continue;
                }
                let mut dst_past = HashSet::new();
                for e in delivered_msgs {
                    if e != msg {
                        dst_past.insert(e.clone());
                    } else {
                        break;
                    }
                }
                if !dst_past.is_superset(&src_past) {
                    let missing = src_past.difference(&dst_past).cloned().collect::<Vec<String>>();
                    println!(
                        "Causal order violation: {} not delivered [{}] before [{}]",
                        dst,
                        missing.join(", "),
                        msg
                    );
                    causal_order = false;
                }
            }
        }
    }

    if no_duplication & no_creation & validity & uniform_agreement & causal_order {
        Ok(true)
    } else {
        let mut violated = Vec::new();
        if !no_duplication {
            violated.push("NO DUPLICATION")
        }
        if !no_creation {
            violated.push("NO CREATION")
        }
        if !validity {
            violated.push("VALIDITY")
        }
        if !uniform_agreement {
            violated.push("UNIFORM AGREEMENT")
        }
        if !causal_order {
            violated.push("CAUSAL ORDER")
        }
        Err(format!("Violated {}", violated.join(", ")))
    }
}

// TESTS -------------------------------------------------------------------------------------------

fn test_normal(config: &TestConfig) -> TestResult {
    let mut sys = build_system(config);
    let msg = Message::new("SEND", &format!(r#"{{"text": "{}"}}"#, "0:Hello"));
    sys.send_local_message("proc-0", msg);
    sys.step_until_no_events();
    check(&mut sys, config)
}

fn test_sender_crash(config: &TestConfig) -> TestResult {
    let mut sys = build_system(config);
    let msg = Message::new("SEND", &format!(r#"{{"text": "{}"}}"#, "0:Hello"));
    sys.send_local_message("proc-0", msg);
    // let 2 messages to deliver (sender and one other node)
    sys.steps(2);
    // crash source node
    sys.crash_node("node-0");
    sys.step_until_no_events();
    check(&mut sys, config)
}

fn test_sender_crash2(config: &TestConfig) -> TestResult {
    let mut sys = build_system(config);
    let msg = Message::new("SEND", &format!(r#"{{"text": "{}"}}"#, "0:Hello"));
    sys.send_local_message("proc-0", msg);
    // let 1 message to deliver (sender only)
    sys.step();
    sys.crash_node("node-0");
    sys.step_until_no_events();
    check(&mut sys, config)
}

fn test_two_crashes(config: &TestConfig) -> TestResult {
    let mut sys = build_system(config);
    // simulate that 0 and 1 communicated only with each other and then crashed
    for n in 2..config.proc_count {
        sys.network().disconnect_node(&format!("node-{}", &n.to_string()));
    }
    let msg = Message::new("SEND", &format!(r#"{{"text": "{}"}}"#, "0:Hello"));
    sys.send_local_message("proc-0", msg);
    sys.steps(config.proc_count.pow(2));
    sys.crash_node("node-0");
    sys.crash_node("node-1");
    sys.step_until_no_events();
    check(&mut sys, config)
}

fn test_two_crashes2(config: &TestConfig) -> TestResult {
    let mut sys = build_system(config);
    // simulate that 1 and 2 communicated only with 0 and then crashed
    sys.network().drop_outgoing("node-1");
    sys.network().drop_outgoing("node-2");
    let msg = Message::new("SEND", &format!(r#"{{"text": "{}"}}"#, "0:Hello"));
    sys.send_local_message("proc-0", msg);
    sys.steps(config.proc_count.pow(2));
    sys.crash_node("node-1");
    sys.crash_node("node-2");
    sys.step_until_no_events();
    check(&mut sys, config)
}

fn test_causal_order(config: &TestConfig) -> TestResult {
    let mut sys = build_system(config);
    sys.network().set_delays(100., 200.);
    let msg = Message::new("SEND", &format!(r#"{{"text": "{}"}}"#, "0:Hello!"));
    sys.send_local_message("proc-0", msg);
    while sys.event_log("proc-1").is_empty() {
        sys.step();
    }
    sys.network().set_delays(10., 20.);
    let msg = Message::new("SEND", &format!(r#"{{"text": "{}"}}"#, "1:How?"));
    sys.send_local_message("proc-1", msg);

    while sys.event_log("proc-0").len() < 3 {
        sys.step();
    }

    sys.network().set_delay(1.);
    let msg = Message::new("SEND", &format!(r#"{{"text": "{}"}}"#, "0:Fine!"));
    sys.send_local_message("proc-1", msg);
    sys.step_until_no_events();
    check(&mut sys, config)
}

fn mc_check_too_deep(state: &McState, depth: u64) -> Result<(), String> {
    if state.search_depth > depth {
        Err("too deep".to_owned())
    } else {
        Ok(())
    }
}


fn mc_prune_proc_permutations(state: &McState) -> Option<String> {
    let mut proc_names = vec![];
    for event in &state.log {
        if let McEvent::MessageReceived { src, .. } = event {
            if proc_names.iter().find(|x| **x == src).is_none() {
                proc_names.push(src);
            }
        }
        if let McEvent::MessageDropped { src, .. } = event {
            if proc_names.iter().find(|x| **x == src).is_none() {
                proc_names.push(src);
            }
        }
    }
    if proc_names.len() <= 1 {
        return None;
    }
    for i in 0..(proc_names.len() - 1) {
        if proc_names[i] > proc_names[i + 1] {
            return Some("state is a permutation of other state".to_owned());
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

    if state.log.iter().filter(|event| 
        match *event {
            McEvent::MessageReceived { .. } => true,
            McEvent::MessageDropped { .. } => true,
            _ => false
        }
    ).count() > total {
        return Some("too many messages in total".to_owned());
    }
    None
}

fn mc_invariant(state: &McState, config: &TestConfig, sent_messages: &HashSet<String>) -> Result<(), String> {
    for i in 0..config.proc_count {
        let outbox = &state.node_states[&format!("node-{}", i)][&format!("proc-{}", i)].local_outbox;
        let mut unique = HashSet::new();
        for message in outbox {
            let data: Value = serde_json::from_str(&message.data).unwrap();
            let message = data["text"].as_str().unwrap().to_string();

            if unique.contains(&message) {
                return Err("No duplication violated".to_owned());
            }
            unique.insert(message.clone());
            if !sent_messages.contains(&message) {
                return Err("No creation violated".to_owned());
            }
        }
    }
    Ok(())
}

fn mc_goal(state: &McState, config: &TestConfig, can_be_crashed: Option<u64>) -> Option<String> {
    let mut received_total = 1;
    for i in 0..config.proc_count {
        let outbox = &state.node_states.get(&format!("node-{}", i)).and_then(|node| node.get(&format!("proc-{}", i)).and_then(|proc| Some(&proc.local_outbox)));
        if let Some(outbox) = outbox {
            if let Some(id_crashed) = can_be_crashed {
                if i == id_crashed {
                    continue;
                }
            }
            received_total = min(received_total, outbox.len());
        } else {
            // ingore because node can be disconnected and that's ok
        }
    }
    if received_total == 1 {
        Some("done".to_owned())
    } else {
        None
    }
}

fn mc_clear_state_history(state: &mut McState) {
    state.log.clear();
    for (node, node_state) in state.node_states.iter_mut() {
        node_state.get_mut(node).map(|y| y.sent_message_count = 0);
        node_state.get_mut(node).map(|y| y.received_message_count = 0);
        node_state.get_mut(node).map(|y| y.local_outbox.clear());
    }
}

fn get_n_start_states(start_states: HashSet<McState>, mut n: usize) -> HashSet<McState> {
    n = min(n, start_states.len());
    let mut hashed = start_states.into_iter().map(|state| {
        let mut hasher = DefaultHasher::default();
        state.hash(&mut hasher);
        (hasher.finish(), state)
    }).collect::<Vec<(u64, McState)>>();
    hashed.sort_by_key(|(a, b)| *a);
    HashSet::from_iter(hashed.split_at(n).0.into_iter().map(|(a, b)| b.clone()))
}

fn test_model_checking_normal_delivery(config: &TestConfig) -> TestResult {
    let mut sys = build_system(config);
    let msg_text = "0:Hello".to_owned();
    let msg = Message::new("SEND", &format!(r#"{{"text": "{}"}}"#, msg_text));
    sys.send_local_message("proc-0", msg.clone());
    let config = *config;
    let test_sent_messages: HashSet<String> = HashSet::from_iter(vec![msg_text].into_iter());
    let mut mc = ModelChecker::new(
        &sys,
        Box::new(Dfs::new(
            Box::new(|state| {
                mc_prune_messages(state, 2, 6).or_else(||
                mc_prune_proc_permutations(state).or_else(||
                mc_check_too_deep(state, 10).err()))
            }),
            Box::new(move |state| {
                mc_goal(state, &config, None)
            }),
            Box::new(move |state| {
                mc_invariant(state, &config, &test_sent_messages)
            }),
            None,
            dslab_mp::mc::strategy::ExecutionMode::Debug,
        )),
    );
    let res = mc.run();
    if res.is_err() {
        return Err(format!("model checher found error: {}", res.as_ref().err().unwrap()));
    }
    let res = res.unwrap();
    println!("{:?}", res.summary);
    if !res.summary.states.contains_key("done") {
        return Err("cant get answers".to_owned());
    }
    Ok(true)
}

fn test_model_checking_sender_crash(config: &TestConfig) -> TestResult {
    let mut sys = build_system(config);
    let msg_text = "0:Hello".to_owned();
    let msg = Message::new("SEND", &format!(r#"{{"text": "{}"}}"#, msg_text));
    sys.send_local_message("proc-0", msg.clone());
    let config = *config;
    let test_sent_messages: HashSet<String> = HashSet::from_iter(vec![msg_text].into_iter());
    let mut mc = ModelChecker::new(
        &sys,
        Box::new(Dfs::new(
            Box::new(|state| {
                mc_check_too_deep(state, 4).err()
            }),
            Box::new(move |state| {
                mc_goal(state, &config, None)
            }),
            Box::new(|state| {
                mc_invariant(state, &config, &test_sent_messages)
            }),
            Some(Box::new(move |state| {
                for i in 1..3 {
                    let process_state = &state.node_states[&format!("node-{}", i)][&format!("proc-{}", i)];
                    if process_state.received_message_count > 0 {
                        return true;
                    }
                }
                false
            })),
            dslab_mp::mc::strategy::ExecutionMode::Debug,
        )),
    );
    let res = mc.run();
    if res.is_err() {
        return Err(format!("model checher found error: {}", res.as_ref().err().unwrap()));
    }
    let res = res.unwrap();
    sys.network().disconnect_node("node-0");
    let mut mc = ModelChecker::new(
        &sys,
        Box::new(Dfs::new(
            Box::new(|state| {
                mc_prune_messages(state, 4, 8).or_else(||
                mc_prune_proc_permutations(state).or_else(||
                mc_check_too_deep(state, 6).err()))
            }),
            Box::new(move |state| {
                mc_goal(state, &config, Some(0))
            }),
            Box::new(|state| {
                mc_invariant(state, &config, &test_sent_messages)
            }),
            None,
            dslab_mp::mc::strategy::ExecutionMode::Debug,
        ))
    );
    if res.collected.is_empty() {
        return Err(format!("can't move to second stage"));
    }
    let collected = get_n_start_states(res.collected, 100);
    let mut res = McResult::default();
    for mut state in collected {
        mc_clear_state_history(&mut state);
        // Model Checking
        let cur_res = mc.run();
        mc.set_state(state);
        if cur_res.is_err() {
            return Err(format!("model checher found error: {}", cur_res.as_ref().err().unwrap()));
        }
        res.combine(cur_res.unwrap());
    }
    println!("{:?}", res.summary);
    if !res.summary.states.contains_key("done") {
        return Err("cant get answers".to_owned());
    }
    Ok(true)
}

fn test_chaos_monkey(config: &TestConfig) -> TestResult {
    let mut rand = Pcg64::seed_from_u64(config.seed);
    for i in 1..=config.monkeys {
        let mut run_config = *config;
        run_config.seed = rand.next_u64();
        println!("- Run {} (seed: {})", i, run_config.seed);
        let mut sys = build_system(config);
        let victim1 = format!("node-{}", rand.gen_range(0..config.proc_count));
        let mut victim2 = format!("node-{}", rand.gen_range(0..config.proc_count));
        while victim2 == victim1 {
            victim2 = format!("node-{}", rand.gen_range(0..config.proc_count));
        }
        for i in 0..10 {
            let user = rand.gen_range(0..config.proc_count).to_string();
            let msg = Message::new("SEND", &format!(r#"{{"text": "{}:{}"}}"#, user, i));
            sys.send_local_message(&format!("proc-{}", user), msg.clone());
            if i % 2 == 0 {
                sys.network().set_delays(10., 20.);
            } else {
                sys.network().set_delays(1., 2.);
            }
            for _ in 1..10 {
                if rand.gen_range(0.0..1.0) > 0.3 {
                    sys.network().drop_outgoing(&victim1);
                } else {
                    sys.network().pass_outgoing(&victim1);
                }
                if rand.gen_range(0.0..1.0) > 0.3 {
                    sys.network().drop_outgoing(&victim2);
                } else {
                    sys.network().pass_outgoing(&victim2);
                }
                sys.steps(rand.gen_range(1..5));
            }
        }
        sys.crash_node(&victim1);
        sys.crash_node(&victim2);
        sys.step_until_no_events();
        check(&mut sys, config)?;
    }
    Ok(true)
}

#[allow(dead_code)]
fn test_scalability(config: &TestConfig) -> TestResult {
    let sys_sizes = [
        config.proc_count,
        config.proc_count * 2,
        config.proc_count * 4,
        config.proc_count * 10,
    ];
    let mut msg_counts = Vec::new();
    for node_count in sys_sizes {
        let mut run_config = *config;
        run_config.proc_count = node_count;
        let mut sys = build_system(&run_config);
        let msg = Message::new("SEND", &format!(r#"{{"text": "{}"}}"#, "0:Hello!"));
        sys.send_local_message("proc-0", msg.clone());
        sys.step_until_no_events();
        msg_counts.push(sys.network().message_count());
    }
    println!("\nMessage count:");
    for i in 0..sys_sizes.len() {
        let baseline = (sys_sizes[i] * (sys_sizes[i] - 1)) as u64;
        println!("- N={}: {} (baseline {})", sys_sizes[i], msg_counts[i], baseline);
    }
    Ok(true)
}

// CLI -----------------------------------------------------------------------------------------------------------------

/// Broadcast Homework Tests
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

    /// Random seed used in tests
    #[clap(long, short, default_value = "123")]
    seed: u64,

    /// Number of nodes used in tests
    #[clap(long, short, default_value = "5")]
    node_count: u64,

    /// Number of chaos monkey runs
    #[clap(long, short, default_value = "10")]
    monkeys: u32,

    /// Path to dslib directory
    #[clap(long = "lib", short = 'l', default_value = "../../dslib")]
    dslib_path: String,
}

// MAIN --------------------------------------------------------------------------------------------

fn main() {
    let args = Args::parse();
    if args.debug {
        init_logger(LevelFilter::Trace);
    }

    env::set_var("PYTHONPATH", "../../crates/dslab-mp-python/python");

    let proc_factory = PyProcessFactory::new(&args.solution_path, "BroadcastProcess");
    let config = TestConfig {
        proc_factory: &proc_factory,
        proc_count: args.node_count,
        seed: args.seed,
        monkeys: args.monkeys,
        debug: args.debug,
    };
    let mut tests = TestSuite::new();

    tests.add("NORMAL", test_normal, config);
    tests.add("SENDER CRASH", test_sender_crash, config);
    tests.add("SENDER CRASH 2", test_sender_crash2, config);
    tests.add("TWO CRASHES", test_two_crashes, config);
    tests.add("TWO CRASHES 2", test_two_crashes2, config);
    tests.add("CAUSAL ORDER", test_causal_order, config);
    let config_mc = TestConfig {
        proc_factory: &proc_factory,
        proc_count: 3,
        seed: args.seed,
        monkeys: args.monkeys,
        debug: args.debug,
    };
    tests.add("CHAOS MONKEY", test_chaos_monkey, config);
    tests.add("SCALABILITY", test_scalability, config);
    tests.add("MODEL CHECKING NORMAL DELIVERY", test_model_checking_normal_delivery, config_mc);
    tests.add("MODEL CHECKING SENDER CRASH", test_model_checking_sender_crash, config_mc);

    if args.test.is_none() {
        tests.run();
    } else {
        tests.run_test(&args.test.unwrap());
    }
}
