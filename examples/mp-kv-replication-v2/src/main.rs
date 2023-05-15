use std::borrow::Cow;
use std::cmp::min;
use std::collections::hash_map::DefaultHasher;
use std::collections::HashSet;
use std::hash::{Hash, Hasher};
use std::io::Write;
use std::{env, num};

use assertables::{assume, assume_eq};
use byteorder::{ByteOrder, LittleEndian};
use clap::Parser;
use dslab_mp::mc::events::McEvent;
use dslab_mp::mc::model_checker::ModelChecker;
use dslab_mp::mc::strategies::dfs::Dfs;
use dslab_mp::mc::strategy::McResult;
use dslab_mp::mc::system::McState;
use env_logger::Builder;
use log::LevelFilter;
use rand::distributions::WeightedIndex;
use rand::prelude::*;
use rand_pcg::Pcg64;
use serde::{Deserialize, Serialize};

use dslab_mp::message::Message;
use dslab_mp::system::System;
use dslab_mp::test::{TestResult, TestSuite};
use dslab_mp_python::PyProcessFactory;

// MESSAGES ----------------------------------------------------------------------------------------

#[derive(Serialize)]
struct GetMessage<'a> {
    key: &'a str,
    quorum: u8,
}

#[derive(Deserialize)]
struct GetRespMessage<'a> {
    key: &'a str,
    values: Vec<&'a str>,
    context: Option<Cow<'a, str>>,
}

#[derive(Serialize)]
struct PutMessage<'a> {
    key: &'a str,
    value: &'a str,
    context: Option<String>,
    quorum: u8,
}

#[derive(Deserialize)]
struct PutRespMessage<'a> {
    key: &'a str,
    values: Vec<&'a str>,
    context: Cow<'a, str>,
}

// UTILS -------------------------------------------------------------------------------------------

#[derive(Copy, Clone)]
struct TestConfig<'a> {
    process_factory: &'a PyProcessFactory,
    process_count: u32,
    seed: u64,
}

fn init_logger(level: LevelFilter) {
    Builder::new()
        .filter(None, level)
        .format(|buf, record| writeln!(buf, "{}", record.args()))
        .init();
}

fn build_system(config: &TestConfig) -> System {
    let mut sys = System::new(config.seed);
    sys.network().set_delays(0.01, 0.1);
    let mut process_names = Vec::new();
    for n in 0..config.process_count {
        process_names.push(format!("{}", n));
    }
    for process_name in process_names.iter() {
        let process = config
            .process_factory
            .build((process_name, process_names.clone()), config.seed);
        let node_name = process_name;
        sys.add_node(node_name);
        sys.add_process(process_name, Box::new(process), process_name);
        let clock_skew = sys.gen_range(0.0..1.0);
        sys.set_node_clock_skew(node_name, clock_skew);
    }
    sys
}

fn check_get(
    sys: &mut System,
    proc: &str,
    key: &str,
    quorum: u8,
    expected: Option<Vec<&str>>,
    max_steps: u32,
) -> Result<(Vec<String>, Option<String>), String> {
    sys.send_local_message(proc, Message::json("GET", &GetMessage { key, quorum }));
    let res = sys.step_until_local_message_max_steps(proc, max_steps);
    assume!(res.is_ok(), format!("GET_RESP is not returned by {}", proc))?;
    let msgs = res.unwrap();
    let msg = msgs.first().unwrap();
    assume_eq!(msg.tip, "GET_RESP")?;
    let data: GetRespMessage = serde_json::from_str(&msg.data).unwrap();
    assume_eq!(data.key, key)?;
    if let Some(expected) = expected {
        let mut values_set: HashSet<_> = data.values.clone().into_iter().collect();
        let mut expected_set: HashSet<_> = expected.into_iter().collect();

        if key.starts_with("CART") || key.starts_with("XCART") {
            assert!(values_set.len() <= 1, "Expected no more than 1 value");
            assert!(expected_set.len() <= 1, "Expected cant contain more than 1 value");
            values_set = values_set
                .into_iter()
                .next()
                .map(|s| s.split(',').collect())
                .unwrap_or_default();
            expected_set = expected_set
                .into_iter()
                .next()
                .map(|s| s.split(',').collect())
                .unwrap_or_default();
        }

        assume_eq!(values_set, expected_set)?;
    }
    Ok((
        data.values.iter().map(|x| x.to_string()).collect(),
        data.context.map(|x| x.to_string()),
    ))
}

fn check_put(
    sys: &mut System,
    proc: &str,
    key: &str,
    value: &str,
    context: Option<String>,
    quorum: u8,
    max_steps: u32,
) -> Result<(Vec<String>, String), String> {
    sys.send_local_message(
        proc,
        Message::json(
            "PUT",
            &PutMessage {
                key,
                value,
                quorum,
                context,
            },
        ),
    );
    let res = sys.step_until_local_message_max_steps(proc, max_steps);
    assume!(res.is_ok(), format!("PUT_RESP is not returned by {}", proc))?;
    let msgs = res.unwrap();
    let msg = msgs.first().unwrap();
    assume_eq!(msg.tip, "PUT_RESP")?;
    let data: PutRespMessage = serde_json::from_str(&msg.data).unwrap();
    assume_eq!(data.key, key)?;
    Ok((
        data.values.iter().map(|x| x.to_string()).collect(),
        data.context.to_string(),
    ))
}

fn send_put(sys: &mut System, proc: &str, key: &str, value: &str, quorum: u8, context: Option<String>) {
    sys.send_local_message(
        proc,
        Message::json(
            "PUT",
            &PutMessage {
                key,
                value,
                quorum,
                context,
            },
        ),
    );
}

fn check_put_result(sys: &mut System, proc: &str, key: &str, max_steps: u32) -> TestResult {
    let res = sys.step_until_local_message_max_steps(proc, max_steps);
    assume!(res.is_ok(), format!("PUT_RESP is not returned by {}", proc))?;
    let msgs = res.unwrap();
    let msg = msgs.first().unwrap();
    assume_eq!(msg.tip, "PUT_RESP")?;
    let data: PutRespMessage = serde_json::from_str(&msg.data).unwrap();
    assume_eq!(data.key, key)?;
    Ok(true)
}

fn check_cart_values(values: &Vec<String>, expected: &HashSet<&str>) -> TestResult {
    assume_eq!(values.len(), 1, "Expected single value")?;
    let items: Vec<&str> = values[0].split(',').collect();
    assume_eq!(
        items.len(),
        expected.len(),
        format!("Expected {} items in the cart", expected.len())
    )?;
    let items_set: HashSet<&str> = HashSet::from_iter(items);
    assume_eq!(items_set, *expected)
}

const SYMBOLS: [char; 36] = [
    'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w',
    'x', 'y', 'z', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
];
const WEIGHTS: [usize; 36] = [
    13, 16, 3, 8, 8, 5, 6, 23, 4, 8, 24, 12, 2, 1, 1, 10, 5, 8, 10, 1, 24, 3, 1, 8, 12, 22, 5, 20, 18, 5, 5, 2, 1, 3,
    16, 22,
];

fn random_string(length: usize, rand: &mut Pcg64) -> String {
    let dist = WeightedIndex::new(WEIGHTS).unwrap();
    rand.sample_iter(&dist).take(length).map(|x| SYMBOLS[x]).collect()
}

fn key_replicas(key: &str, sys: &System) -> Vec<String> {
    let process_count = sys.process_names().len();
    let mut replicas = Vec::new();
    let hash = md5::compute(key);
    let hash128 = LittleEndian::read_u128(&hash.0);
    let mut replica = (hash128 % process_count as u128) as usize;
    for _ in 0..3 {
        replicas.push(replica.to_string());
        replica += 1;
        if replica == process_count {
            replica = 0;
        }
    }
    replicas
}

fn key_non_replicas(key: &str, sys: &System) -> Vec<String> {
    let replicas = key_replicas(key, sys);
    let mut non_replicas_pre = Vec::new();
    let mut non_replicas = Vec::new();
    let mut pre = true;
    for proc in sys.process_names_sorted() {
        if replicas.contains(&proc) {
            pre = false;
            continue;
        }
        if pre {
            non_replicas_pre.push(proc);
        } else {
            non_replicas.push(proc);
        }
    }
    non_replicas.append(&mut non_replicas_pre);
    non_replicas
}

// TESTS -------------------------------------------------------------------------------------------

fn test_basic(config: &TestConfig) -> TestResult {
    let mut sys = build_system(config);
    let procs = sys.process_names();
    let mut rand = Pcg64::seed_from_u64(config.seed);

    let key = random_string(8, &mut rand).to_uppercase();
    let replicas = key_replicas(&key, &sys);
    let non_replicas = key_non_replicas(&key, &sys);
    println!("Key {} replicas: {:?}", key, replicas);

    // get key from the first process
    check_get(&mut sys, &procs[0], &key, 2, Some(vec![]), 100)?;

    // put key from the first replica
    let value = random_string(8, &mut rand);
    let (values, _) = check_put(&mut sys, &replicas[0], &key, &value, None, 2, 100)?;
    assume_eq!(values.len(), 1, "Expected single value")?;
    assume_eq!(values[0], value)?;

    // get key from the last replica
    check_get(&mut sys, &replicas[2], &key, 2, Some(vec![&value]), 100)?;

    // get key from the first non-replica
    check_get(&mut sys, &non_replicas[0], &key, 2, Some(vec![&value]), 100)?;

    // update key from the last non-replica
    let (_, ctx) = check_get(&mut sys, &non_replicas[2], &key, 2, Some(vec![&value]), 100)?;
    let value2 = random_string(8, &mut rand);
    let (values, _) = check_put(&mut sys, &non_replicas[2], &key, &value2, ctx, 2, 100)?;
    assume_eq!(values.len(), 1, "Expected single value")?;
    assume_eq!(values[0], value2)?;

    // get key from the first process
    check_get(&mut sys, &procs[0], &key, 2, Some(vec![&value2]), 100)?;
    Ok(true)
}

fn test_stale_replica(config: &TestConfig) -> TestResult {
    let mut sys = build_system(config);
    let mut rand = Pcg64::seed_from_u64(config.seed);

    let key = random_string(8, &mut rand).to_uppercase();
    let replicas = key_replicas(&key, &sys);
    let non_replicas = key_non_replicas(&key, &sys);

    // put key from the first non-replica with quorum 3
    let value = random_string(8, &mut rand);
    check_put(&mut sys, &non_replicas[0], &key, &value, None, 3, 100)?;

    // disconnect the first replica
    sys.network().disconnect_node(&replicas[0]);

    // update key from the last replica with quorum 2
    let (_, ctx) = check_get(&mut sys, &replicas[2], &key, 2, Some(vec![&value]), 100)?;
    let value2 = random_string(8, &mut rand);
    check_put(&mut sys, &replicas[2], &key, &value2, ctx, 2, 100)?;

    // disconnect the last replica
    sys.network().disconnect_node(&replicas[2]);
    // connect the first replica
    sys.network().connect_node(&replicas[0]);

    // read key from the second replica with quorum 2
    check_get(&mut sys, &replicas[1], &key, 2, Some(vec![&value2]), 100)?;

    // step for a while and check whether the first replica got the recent value
    sys.steps(100);
    sys.network().disconnect_node(&replicas[0]);
    check_get(&mut sys, &replicas[0], &key, 1, Some(vec![&value2]), 100)?;
    Ok(true)
}

fn test_concurrent_writes_1(config: &TestConfig) -> TestResult {
    let mut sys = build_system(config);
    let mut rand = Pcg64::seed_from_u64(config.seed);

    let key = random_string(8, &mut rand).to_uppercase();
    let non_replicas = key_non_replicas(&key, &sys);
    let proc_1 = &non_replicas.get(0).unwrap();
    let proc_2 = &non_replicas.get(1).unwrap();
    let proc_3 = &non_replicas.get(2).unwrap();

    // put key from proc_1 (quorum=2)
    let value1 = random_string(8, &mut rand);
    let (values, _) = check_put(&mut sys, proc_1, &key, &value1, None, 2, 100)?;
    assume_eq!(values.len(), 1, "Expected single value")?;
    assume_eq!(values[0], value1)?;

    // concurrently (using same context) put key from proc_2 (quorum=2)
    let value2 = random_string(8, &mut rand);
    let (values, _) = check_put(&mut sys, proc_2, &key, &value2, None, 2, 100)?;
    assume_eq!(values.len(), 2, "Expected two values")?;

    // read key from proc_3 (quorum=2)
    // should return both values for reconciliation by the client
    check_get(&mut sys, proc_3, &key, 2, Some(vec![&value1, &value2]), 100)?;
    Ok(true)
}

fn test_concurrent_writes_2(config: &TestConfig) -> TestResult {
    let mut sys = build_system(config);
    let mut rand = Pcg64::seed_from_u64(config.seed);

    let key = random_string(8, &mut rand).to_uppercase();
    let non_replicas = key_non_replicas(&key, &sys);
    let proc_1 = &non_replicas.get(0).unwrap();
    let proc_2 = &non_replicas.get(1).unwrap();
    let proc_3 = &non_replicas.get(2).unwrap();

    // put key from proc_1 (quorum=2)
    let value1 = random_string(8, &mut rand);
    send_put(&mut sys, proc_1, &key, &value1, 2, None);

    // concurrently (using same context) put key from proc_2 (quorum=2)
    let value2 = random_string(8, &mut rand);
    send_put(&mut sys, proc_2, &key, &value2, 2, None);

    // wait until both puts are processed
    check_put_result(&mut sys, proc_1, &key, 100)?;
    check_put_result(&mut sys, proc_2, &key, 100)?;

    // read key from proc_3 (quorum=2)
    // should return both values for reconciliation by the client
    let (_, ctx) = check_get(&mut sys, proc_3, &key, 2, Some(vec![&value1, &value2]), 100)?;
    // put new reconciled value using the obtained context
    let value3 = [value1, value2].join("+");
    check_put(&mut sys, proc_3, &key, &value3, ctx, 2, 100)?;

    // read key from proc_1 (quorum=2)
    check_get(&mut sys, proc_1, &key, 2, Some(vec![&value3]), 100)?;
    Ok(true)
}

fn test_concurrent_writes_3(config: &TestConfig) -> TestResult {
    let mut sys = build_system(config);
    let mut rand = Pcg64::seed_from_u64(config.seed);

    let key = random_string(8, &mut rand).to_uppercase();
    let replicas = key_replicas(&key, &sys);
    let non_replicas = key_non_replicas(&key, &sys);

    // put key from the first replica (quorum=1)
    let value1 = random_string(8, &mut rand);
    check_put(&mut sys, &replicas[0], &key, &value1, None, 1, 100)?;

    // concurrently put key from the second replica (quorum=1)
    let value2 = random_string(8, &mut rand);
    check_put(&mut sys, &replicas[1], &key, &value2, None, 1, 100)?;

    // read key from the first non-replica (quorum=3)
    check_get(&mut sys, &non_replicas[0], &key, 3, Some(vec![&value1, &value2]), 100)?;
    Ok(true)
}

fn test_diverged_replicas(config: &TestConfig) -> TestResult {
    let mut sys = build_system(config);
    let mut rand = Pcg64::seed_from_u64(config.seed);

    let key = random_string(8, &mut rand).to_uppercase();
    let replicas = key_replicas(&key, &sys);
    let non_replicas = key_non_replicas(&key, &sys);

    // put key from the first replica with quorum 3
    let value = random_string(8, &mut rand);
    check_put(&mut sys, &replicas[0], &key, &value, None, 3, 100)?;

    // disconnect each replica and put value from it
    let mut new_values = Vec::new();
    for replica in replicas.iter() {
        sys.network().disconnect_node(replica);
    }
    for replica in replicas.iter() {
        let (_, ctx) = check_get(&mut sys, replica, &key, 1, Some(vec![&value]), 100)?;
        let value2 = random_string(8, &mut rand);
        check_put(&mut sys, replica, &key, &value2, ctx, 1, 100)?;
        new_values.push(value2);
        // read some key to advance the time
        // (make sure that the isolated replicas are not among this key's replicas)
        loop {
            let some_key = random_string(8, &mut rand).to_uppercase();
            let some_key_replicas = key_replicas(&some_key, &sys);
            if replicas.iter().all(|proc| !some_key_replicas.contains(proc)) {
                check_get(&mut sys, &non_replicas[0], &some_key, 3, Some(vec![]), 100)?;
                break;
            }
        }
    }

    // reconnect the replicas
    for replica in replicas.iter() {
        sys.network().connect_node(replica);
    }

    // read key from the first replica with quorum 3
    // should return all three conflicting values
    let expected = new_values.iter().map(String::as_str).collect();
    check_get(&mut sys, &replicas[0], &key, 3, Some(expected), 100)?;
    Ok(true)
}

fn test_sloppy_quorum(config: &TestConfig) -> TestResult {
    let mut sys = build_system(config);
    let mut rand = Pcg64::seed_from_u64(config.seed);

    let key = random_string(8, &mut rand).to_uppercase();
    let replicas = key_replicas(&key, &sys);
    let non_replicas = key_non_replicas(&key, &sys);

    // put key from the first non-replica with quorum 3
    let value = random_string(8, &mut rand);
    check_put(&mut sys, &non_replicas[0], &key, &value, None, 3, 100)?;

    // temporarily disconnect the first replica
    sys.network().disconnect_node(&replicas[0]);

    // update key from the second non-replica with quorum 3 (should use sloppy quorum)
    let (_, ctx) = check_get(&mut sys, &non_replicas[1], &key, 1, Some(vec![&value]), 100)?;
    let value2 = random_string(8, &mut rand);
    check_put(&mut sys, &non_replicas[1], &key, &value2, ctx, 3, 100)?;

    // read key from the last non-replica with quorum 3 (should use sloppy quorum)
    check_get(&mut sys, &non_replicas[2], &key, 3, Some(vec![&value2]), 100)?;

    // reconnect the first replica and let it receive the update
    sys.network().connect_node(&replicas[0]);
    sys.steps(100);

    // check if the first replica got update
    sys.network().disconnect_node(&replicas[0]);
    check_get(&mut sys, &replicas[0], &key, 1, Some(vec![&value2]), 100)?;
    Ok(true)
}

fn test_partitioned_clients(config: &TestConfig) -> TestResult {
    let mut sys = build_system(config);
    let procs = sys.process_names();
    let mut rand = Pcg64::seed_from_u64(config.seed);

    let key = random_string(8, &mut rand).to_uppercase();
    let replicas = key_replicas(&key, &sys);
    let replica1 = &replicas[0];
    let replica2 = &replicas[1];
    let replica3 = &replicas[2];
    let non_replicas = key_non_replicas(&key, &sys);
    let non_replica1 = &non_replicas[0];
    let non_replica2 = &non_replicas[1];
    let non_replica3 = &non_replicas[2];

    // put key from the first process with quorum 3
    let value = random_string(8, &mut rand);
    check_put(&mut sys, &procs[0], &key, &value, None, 3, 100)?;

    // partition processes into two parts
    let part1: Vec<&str> = vec![non_replica1, non_replica2, replica1];
    let part2: Vec<&str> = vec![non_replica3, replica2, replica3];
    sys.network().make_partition(&part1, &part2);

    // partition 1
    let (values, ctx) = check_get(&mut sys, non_replica1, &key, 2, Some(vec![&value]), 100)?;
    let mut value2 = format!("{}-1", values[0]);
    check_put(&mut sys, non_replica1, &key, &value2, ctx, 2, 100)?;
    let (values, ctx) = check_get(&mut sys, non_replica2, &key, 2, Some(vec![&value2]), 100)?;
    value2 = format!("{}-2", values[0]);
    check_put(&mut sys, non_replica2, &key, &value2, ctx, 2, 100)?;
    check_get(&mut sys, non_replica2, &key, 2, Some(vec![&value2]), 100)?;

    // partition 2
    let (values, ctx) = check_get(&mut sys, non_replica3, &key, 2, Some(vec![&value]), 100)?;
    let value3 = format!("{}-3", values[0]);
    check_put(&mut sys, non_replica3, &key, &value3, ctx, 2, 100)?;
    check_get(&mut sys, non_replica3, &key, 2, Some(vec![&value3]), 100)?;

    // heal partition
    sys.network().reset_network();
    sys.steps(100);

    // read key from all non-replicas
    // (should return value2 and value3)
    let expected: Option<Vec<&str>> = Some(vec![&value2, &value3]);
    check_get(&mut sys, non_replica1, &key, 2, expected.clone(), 100)?;
    check_get(&mut sys, non_replica2, &key, 2, expected.clone(), 100)?;
    check_get(&mut sys, non_replica3, &key, 2, expected.clone(), 100)?;

    // check all replicas
    for replica in replicas.iter() {
        sys.network().disconnect_node(replica);
        check_get(&mut sys, replica, &key, 1, expected.clone(), 100)?;
    }
    Ok(true)
}

fn test_shopping_cart_1(config: &TestConfig) -> TestResult {
    let mut sys = build_system(config);
    let mut rand = Pcg64::seed_from_u64(config.seed);

    let key = format!("cart-{}", random_string(8, &mut rand)).to_uppercase();
    let non_replicas = key_non_replicas(&key, &sys);
    let proc_1 = &non_replicas[0];
    let proc_2 = &non_replicas[1];

    // proc_1: + milk
    let mut cart1 = vec!["milk"];
    let (values, ctx1) = check_put(&mut sys, proc_1, &key, &cart1.join(","), None, 2, 100)?;
    assume_eq!(values.len(), 1, "Expected single value")?;
    cart1 = values[0].split(',').collect();

    // proc_2: + eggs
    let mut cart2 = vec!["eggs"];
    let (values, ctx2) = check_put(&mut sys, proc_2, &key, &cart2.join(","), None, 2, 100)?;
    assume_eq!(values.len(), 1, "Expected single value")?;
    cart2 = values[0].split(',').collect();

    // proc_1: + flour
    cart1.push("flour");
    let (values, ctx1) = check_put(&mut sys, proc_1, &key, &cart1.join(","), Some(ctx1), 2, 100)?;
    assume_eq!(values.len(), 1, "Expected single value")?;
    cart1 = values[0].split(',').collect();

    // proc_2: + ham
    cart2.push("ham");
    let (values, _) = check_put(&mut sys, proc_2, &key, &cart2.join(","), Some(ctx2), 2, 100)?;
    assume_eq!(values.len(), 1, "Expected single value")?;

    // proc_1: + flour
    cart1.push("bacon");
    let (values, _) = check_put(&mut sys, proc_1, &key, &cart1.join(","), Some(ctx1), 2, 100)?;
    assume_eq!(values.len(), 1, "Expected single value")?;

    // read cart from all non-replicas
    let expected: HashSet<_> = vec!["milk", "eggs", "flour", "ham", "bacon"].into_iter().collect();
    for proc in non_replicas.iter() {
        let (values, _) = check_get(&mut sys, proc, &key, 2, None, 100)?;
        check_cart_values(&values, &expected)?;
    }
    Ok(true)
}

fn test_shopping_cart_2(config: &TestConfig) -> TestResult {
    let mut sys = build_system(config);
    let mut rand = Pcg64::seed_from_u64(config.seed);

    let key = format!("cart-{}", random_string(8, &mut rand)).to_uppercase();

    let replicas = key_replicas(&key, &sys);
    let replica1 = &replicas[0];
    let replica2 = &replicas[1];
    let replica3 = &replicas[2];
    let non_replicas = key_non_replicas(&key, &sys);
    let proc_1 = &non_replicas[0];
    let proc_2 = &non_replicas[1];
    let proc_3 = &non_replicas[2];

    // proc_1: [beer, snacks]
    let cart0 = vec!["beer", "snacks"];
    let (_, ctx) = check_put(&mut sys, proc_1, &key, &cart0.join(","), None, 3, 100)?;

    // partition processes into two parts
    let part1: Vec<&str> = vec![proc_1, proc_2, replica1];
    let part2: Vec<&str> = vec![proc_3, replica2, replica3];
    sys.network().make_partition(&part1, &part2);

    // partition 1 -----------------------------------------------------------------------------------------------------

    // proc_1: + milk
    let mut cart1 = cart0.clone();
    cart1.push("milk");
    check_put(&mut sys, proc_1, &key, &cart1.join(","), Some(ctx), 2, 100)?;
    // proc_2: read, put [eggs]
    let (values, ctx) = check_get(&mut sys, proc_2, &key, 2, Some(vec![&cart1.join(",")]), 100)?;
    let mut cart2: Vec<_> = values[0].split(',').collect();
    cart2.push("eggs");
    check_put(&mut sys, proc_2, &key, &cart2.join(","), ctx, 2, 100)?;
    // control read
    check_get(&mut sys, proc_1, &key, 2, Some(vec![&cart2.join(",")]), 100)?;

    // partition 2 -----------------------------------------------------------------------------------------------------

    // proc_3: read, remove [snacks, beer], put [cheese, wine]
    let (values, ctx) = check_get(&mut sys, proc_3, &key, 2, Some(vec![&cart0.join(",")]), 100)?;
    let mut cart3: Vec<_> = values[0].split(',').collect();
    cart3.clear();
    cart3.push("cheese");
    cart3.push("wine");
    check_put(&mut sys, proc_3, &key, &cart3.join(","), ctx, 2, 100)?;
    // control read
    check_get(&mut sys, replica2, &key, 2, Some(vec![&cart3.join(",")]), 100)?;

    // heal partition --------------------------------------------------------------------------------------------------
    sys.network().reset_network();
    sys.steps(100);

    // read key from all non-replica processes
    let expected: HashSet<_> = vec!["cheese", "wine", "milk", "eggs", "beer", "snacks"]
        .into_iter()
        .collect();
    for proc in non_replicas.iter() {
        let (values, _) = check_get(&mut sys, proc, &key, 2, None, 100)?;
        check_cart_values(&values, &expected)?;
    }

    // check all replicas
    for replica in replicas.iter() {
        sys.network().disconnect_node(replica);
        let (values, _) = check_get(&mut sys, replica, &key, 1, None, 100)?;
        check_cart_values(&values, &expected)?;
    }
    Ok(true)
}

fn test_shopping_xcart_1(config: &TestConfig) -> TestResult {
    let mut sys = build_system(config);
    let mut rand = Pcg64::seed_from_u64(config.seed);

    let key = format!("xcart-{}", random_string(8, &mut rand)).to_uppercase();
    let replicas = key_replicas(&key, &sys);
    let replica1 = &replicas[0];
    let replica2 = &replicas[1];
    let replica3 = &replicas[2];
    let non_replicas = key_non_replicas(&key, &sys);
    let proc_1 = &non_replicas[0];
    let proc_2 = &non_replicas[1];
    let proc_3 = &non_replicas[2];

    // proc_1: [beer, snacks]
    let cart0 = vec!["beer", "snacks"];
    let (_, ctx) = check_put(&mut sys, proc_1, &key, &cart0.join(","), None, 3, 100)?;

    // partition processes into two parts
    let part1: Vec<&str> = vec![proc_1, proc_2, replica1];
    let part2: Vec<&str> = vec![proc_3, replica2, replica3];
    sys.network().make_partition(&part1, &part2);

    // partition 1 -----------------------------------------------------------------------------------------------------

    // proc_1: put [milk]
    let mut cart1 = cart0.clone();
    cart1.push("milk");
    check_put(&mut sys, proc_1, &key, &cart1.join(","), Some(ctx), 2, 100)?;
    // proc_2: read, put [eggs]
    let (values, ctx) = check_get(&mut sys, proc_2, &key, 2, Some(vec![&cart1.join(",")]), 100)?;
    let mut cart2: Vec<_> = values[0].split(',').collect();
    cart2.push("eggs");
    check_put(&mut sys, proc_2, &key, &cart2.join(","), ctx, 2, 100)?;
    // control read
    check_get(&mut sys, proc_1, &key, 2, Some(vec![&cart2.join(",")]), 100)?;

    // partition 2 -----------------------------------------------------------------------------------------------------

    // proc_3: read, remove [snacks, beer], put [cheese, wine]
    let (values, ctx) = check_get(&mut sys, proc_3, &key, 2, Some(vec![&cart0.join(",")]), 100)?;
    let mut cart3: Vec<_> = values[0].split(',').collect();
    cart3.clear();
    cart3.push("cheese");
    cart3.push("wine");
    check_put(&mut sys, proc_3, &key, &cart3.join(","), ctx, 2, 100)?;
    // control read
    check_get(&mut sys, replica2, &key, 2, Some(vec![&cart3.join(",")]), 100)?;

    // heal partition --------------------------------------------------------------------------------------------------
    sys.network().reset_network();
    sys.steps(100);

    // read key from all non-replica processes
    let expected: HashSet<_> = vec!["cheese", "wine", "milk", "eggs"].into_iter().collect();
    for proc in non_replicas.iter() {
        let (values, _) = check_get(&mut sys, proc, &key, 2, None, 100)?;
        check_cart_values(&values, &expected)?;
    }

    // check all replicas
    for replica in replicas.iter() {
        sys.network().disconnect_node(replica);
        let (values, _) = check_get(&mut sys, replica, &key, 1, None, 100)?;
        check_cart_values(&values, &expected)?;
    }
    Ok(true)
}

fn test_shopping_xcart_2(config: &TestConfig) -> TestResult {
    let mut sys = build_system(config);
    let mut rand = Pcg64::seed_from_u64(config.seed);

    let key = format!("xcart-{}", random_string(8, &mut rand)).to_uppercase();
    let replicas = key_replicas(&key, &sys);
    let replica1 = &replicas[0];
    let replica2 = &replicas[1];
    let replica3 = &replicas[2];
    let non_replicas = key_non_replicas(&key, &sys);
    let proc_1 = &non_replicas[0];
    let proc_2 = &non_replicas[1];
    let proc_3 = &non_replicas[2];

    // proc_1: put [lemonade, snacks, beer]
    let cart0 = vec!["lemonade", "snacks", "beer"];
    let (_, ctx) = check_put(&mut sys, proc_1, &key, &cart0.join(","), None, 3, 100)?;

    // partition processes into two parts
    let part1: Vec<&str> = vec![proc_1, proc_2, replica1];
    let part2: Vec<&str> = vec![proc_3, replica2, replica3];
    sys.network().make_partition(&part1, &part2);

    // partition 1 -----------------------------------------------------------------------------------------------------

    // proc_1: remove lemonade, put milk
    let mut cart1 = cart0.clone();
    cart1.remove(0);
    cart1.push("milk");
    check_put(&mut sys, proc_1, &key, &cart1.join(","), Some(ctx), 2, 100)?;
    // proc_2: read, + eggs
    let (values, ctx) = check_get(&mut sys, proc_2, &key, 2, Some(vec![&cart1.join(",")]), 100)?;
    let mut cart2: Vec<_> = values[0].split(',').collect();
    cart2.push("eggs");
    check_put(&mut sys, proc_2, &key, &cart2.join(","), ctx, 2, 100)?;
    // control read
    check_get(&mut sys, proc_1, &key, 2, Some(vec![&cart2.join(",")]), 100)?;

    // partition 2 -----------------------------------------------------------------------------------------------------

    // proc_3: read, remove [snacks, beer], put [cheese, wine], put snacks (back)
    let (values, ctx) = check_get(&mut sys, proc_3, &key, 2, Some(vec![&cart0.join(",")]), 100)?;
    let mut cart3: Vec<_> = values[0].split(',').collect();
    cart3.clear();
    cart3.push("lemonade");
    cart3.push("cheese");
    cart3.push("wine");
    let (_, ctx) = check_put(&mut sys, proc_3, &key, &cart3.join(","), ctx, 2, 100)?;
    cart3.push("snacks");
    check_put(&mut sys, proc_3, &key, &cart3.join(","), Some(ctx), 2, 100)?;
    // control read
    check_get(&mut sys, replica2, &key, 2, Some(vec![&cart3.join(",")]), 100)?;

    // heal partition --------------------------------------------------------------------------------------------------
    sys.network().reset_network();
    sys.steps(100);

    // read key from all non-replica processes
    let expected: HashSet<_> = vec!["milk", "eggs", "wine", "snacks", "cheese"].into_iter().collect();
    for proc in non_replicas.iter() {
        let (values, _) = check_get(&mut sys, proc, &key, 2, None, 100)?;
        check_cart_values(&values, &expected)?;
    }

    // check all replicas
    for replica in replicas.iter() {
        sys.network().disconnect_node(replica);
        let (values, _) = check_get(&mut sys, replica, &key, 1, None, 100)?;
        check_cart_values(&values, &expected)?;
    }
    Ok(true)
}

fn mc_query_prune<'a>(
    max_timers_allowed: u64,
    max_messages_allowed: u64,
) -> Box<dyn Fn(&McState) -> Option<String> + 'a> {
    Box::new(move |state: &McState| {
        let mut num_timers = 0;
        let mut num_messages = 0;

        for event in &state.log {
            if let McEvent::TimerFired { .. } = event {
                num_timers += 1;
            }
            if let McEvent::MessageReceived { .. } = event {
                num_messages += 1;
            }
            if let McEvent::MessageDropped { .. } = event {
                num_messages += 1;
            }
        }
        if num_timers > max_timers_allowed {
            return Some("too many fired timers".to_owned());
        }
        if num_messages > max_messages_allowed {
            Some("too many messages sent".to_owned())
        } else {
            None
        }
    })
}

fn mc_prune_none<'a>() -> Box<dyn Fn(&McState) -> Option<String> + 'a> {
    Box::new(|_| None)
}

fn mc_invariant_ok<'a>() -> Box<dyn Fn(&McState) -> Result<(), String> + 'a> {
    Box::new(|_| Ok(()))
}

fn mc_goal_query_finished<'a>(node: &'a str, proc: &'a str) -> Box<dyn Fn(&McState) -> Option<String> + 'a> {
    Box::new(move |state| {
        let messages = &state.node_states[node][proc].local_outbox;
        if messages.is_empty() {
            return None;
        }
        Some("request finished".to_owned())
    })
}

fn mc_goal_depth<'a>(max_depth: u64) -> Box<dyn Fn(&McState) -> Option<String> + 'a> {
    Box::new(move |state: &McState| {
        if mc_state_explored(max_depth)(state) {
            Some("explored".to_owned())
        } else {
            None
        }
    })
}

fn mc_state_explored<'a>(max_depth: u64) -> Box<dyn Fn(&McState) -> bool + 'a> {
    Box::new(move |state: &McState| {
        if state.search_depth == max_depth || state.events.available_events_num() == 0 {
            true
        } else {
            false
        }
    })
}

fn sorted_cart(cart: &Vec<&str>) -> Vec<String> {
    let mut res = vec![];
    for items in cart {
        let mut items = items.split(',').map(|s| s.to_owned()).collect::<Vec<String>>();
        items.sort();
        res.push(items.join(","));
    }
    res.sort();
    res
}

fn mc_get_invariant<'a>(
    node: &'a str,
    proc: &'a str,
    key: &'a str,
    mut expected: Vec<&'a str>,
    max_steps: Option<u32>,
) -> Box<dyn Fn() -> Box<dyn Fn(&McState) -> Result<(), String> + 'a> + 'a> {
    Box::new(move || {
        let expected = expected.clone();
        Box::new(move |state: &McState| -> Result<(), String> {
            let messages = &state.node_states[node][proc].local_outbox;
            if let Some(message) = messages.get(0) {
                if message.tip != "GET_RESP" {
                    return Err(format!("wrong type {}", message.tip));
                }
                let mut data: GetRespMessage = serde_json::from_str(&message.data).map_err(|err| err.to_string())?;
                if data.key != key {
                    return Err(format!("wrong key {}", data.key));
                }
                if sorted_cart(&data.values) != sorted_cart(&expected) {
                    println!("{:?}", expected);
                    return Err(format!("wrong value {:?}", data.values));
                }
            } else if let Some(max_steps) = max_steps {
                if state.search_depth > max_steps as u64 {
                    return Err(format!("nothing found but already should be"));
                }
            }
            Ok(())
        })
    })
}

fn mc_put_invariant<'a>(
    node: &'a str,
    proc: &'a str,
    key: &'a str,
    value: &'a Vec<&'a str>,
    max_steps: Option<u32>,
) -> Box<dyn Fn() -> Box<dyn Fn(&McState) -> Result<(), String> + 'a> + 'a> {
    Box::new(move || {
        Box::new(move |state: &McState| -> Result<(), String> {
            let messages = &state.node_states[node][proc].local_outbox;
            if let Some(message) = messages.get(0) {
                if message.tip != "PUT_RESP" {
                    return Err(format!("wrong type {}", message.tip));
                }
                let data: PutRespMessage = serde_json::from_str(&message.data).map_err(|err| err.to_string())?;
                if data.key != key {
                    return Err(format!("wrong key {}", data.key));
                }
                if data.values != *value {
                    return Err(format!("wrong value {:?}", data.values));
                }
            } else if let Some(max_steps) = max_steps {
                if state.search_depth == max_steps as u64 {
                    return Err(format!("nothing found but already should be"));
                }
            }
            Ok(())
        })
    })
}

fn mc_query_collect<'a>(node: &'a str, proc: &'a str) -> Box<dyn Fn(&McState) -> bool + 'a> {
    Box::new(move |state: &McState| {
        let messages = &state.node_states[node][proc].local_outbox;
        !messages.is_empty()
    })
}

fn mc_clear_state_history(state: &mut McState) {
    state.log.clear();
    for (node, node_state) in state.node_states.iter_mut() {
        node_state.get_mut(node).map(|y| y.sent_message_count = 0);
        node_state.get_mut(node).map(|y| y.received_message_count = 0);
        node_state.get_mut(node).map(|y| y.local_outbox.clear());
    }
}

fn mc_check_query<'a>(
    sys: &'a mut System,
    node: &'a str,
    proc: &'a str,
    invariant: Box<dyn Fn() -> Box<dyn Fn(&McState) -> Result<(), String> + 'a> + 'a>,
    msg: Message,
    prune_steps: u32,
    start_states: Option<HashSet<McState>>,
) -> Result<McResult, String> {
    if let Some(start_states) = start_states {
        let mut combined_result = McResult::default();
        for mut start_state in start_states {
            mc_clear_state_history(&mut start_state);
            let mut mc = ModelChecker::new(
                &sys,
                Box::new(Dfs::new(
                    mc_query_prune(2, (prune_steps - 2) as u64),
                    mc_goal_query_finished(node, proc),
                    invariant(),
                    Some(mc_query_collect(node, proc)),
                    dslab_mp::mc::strategy::ExecutionMode::Debug,
                )),
            );
            mc.set_state(start_state);
            mc.apply_event(McEvent::LocalMessageReceived {
                msg: msg.clone(),
                dest: proc.to_string(),
            });
            let res = mc.run();
            if res.is_err() {
                return Err(format!("model checher found error: {}", res.as_ref().err().unwrap()));
            }
            combined_result.combine(res.unwrap());
        }
        println!("{:?}", combined_result.summary);
        Ok(combined_result)
    } else {
        let mut mc = ModelChecker::new(
            &sys,
            Box::new(Dfs::new(
                mc_query_prune(2, (prune_steps - 2) as u64),
                mc_goal_query_finished(node, proc),
                invariant(),
                Some(mc_query_collect(node, proc)),
                dslab_mp::mc::strategy::ExecutionMode::Debug,
            )),
        );
        mc.apply_event(McEvent::LocalMessageReceived {
            msg,
            dest: proc.to_string(),
        });
        let res = mc.run();
        if res.is_err() {
            return Err(format!("model checher found error: {}", res.as_ref().err().unwrap()));
        }
        println!("{:?}", res.clone().unwrap().summary);
        Ok(res.unwrap())
    }
}

fn check_mc_get(
    sys: &mut System,
    node: &str,
    proc: &str,
    key: &str,
    expected: Vec<&str>,
    quorum: u8,
    prune_steps: u32,
    max_steps: Option<u32>,
    start_states: Option<HashSet<McState>>,
) -> Result<McResult, String> {
    println!("check_mc_get");
    let msg = Message::json("GET", &GetMessage { key, quorum });
    mc_check_query(
        sys,
        node,
        proc,
        mc_get_invariant(node, proc, key, expected, max_steps),
        msg,
        prune_steps,
        start_states,
    )
}

fn check_mc_put(
    sys: &mut System,
    node: &str,
    proc: &str,
    key: &str,
    value: &str,
    expected: Vec<&str>,
    context: Option<String>,
    quorum: u8,
    prune_steps: u32,
    max_steps: Option<u32>,
    start_states: Option<HashSet<McState>>,
) -> Result<McResult, String> {
    println!("check_mc_put");
    let msg = Message::json(
        "PUT",
        &PutMessage {
            key,
            value,
            context,
            quorum,
        },
    );
    mc_check_query(
        sys,
        node,
        proc,
        mc_put_invariant(node, proc, key, &expected, max_steps),
        msg,
        prune_steps,
        start_states,
    )
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

fn mc_stabilize(sys: &mut System, num_steps: u64, start_states: Option<HashSet<McState>>) -> Result<McResult, String> {
    println!("mc_stabilize");

    if let Some(start_states) = start_states {
        let mut start_states_updated = HashSet::new();
        for start_state in start_states {
            let mut start_state_updated = start_state.clone();
            for event_id in start_state.events.available_events() {
                if let Some(McEvent::MessageDropped { .. }) = start_state.events.get(event_id) {
                    start_state_updated.events.pop(event_id);
                } else if let Some(McEvent::TimerCancelled { .. }) = start_state.events.get(event_id) {
                    start_state_updated.events.pop(event_id);
                } else {
                    continue;
                }
            }
            start_states_updated.insert(start_state_updated);
        }
        let mut combined_result = McResult::default();
        for start_state in start_states_updated {
            // let start_state = start_states.into_iter().next().unwrap();
            let mut mc = ModelChecker::new(
                &sys,
                Box::new(Dfs::new(
                    mc_query_prune(5, num_steps + 1 - 5),
                    mc_goal_depth(num_steps),
                    mc_invariant_ok(),
                    Some(Box::new(|state| {
                        mc_state_explored(num_steps)(state) || mc_query_prune(5, num_steps + 1 - 5)(state).is_some()
                    })),
                    dslab_mp::mc::strategy::ExecutionMode::Debug,
                )),
            );
            mc.set_state(start_state);
            let res = mc.run();
            if res.is_err() {
                return Err(format!("model checher found error: {}", res.as_ref().err().unwrap()));
            }
            combined_result.combine(res.unwrap());
        }
        println!("{:?}", combined_result.summary);
        Ok(combined_result)
    } else {
        let mut mc = ModelChecker::new(
            &sys,
            Box::new(Dfs::new(
                mc_prune_none(),
                mc_goal_depth(num_steps),
                mc_invariant_ok(),
                Some(mc_state_explored(num_steps)),
                dslab_mp::mc::strategy::ExecutionMode::Debug,
            )),
        );
        let res = mc.run();
        if res.is_err() {
            return Err(format!("model checher found error: {}", res.as_ref().err().unwrap()));
        }
        println!("{:?}", res.clone().unwrap().summary);
        Ok(res.unwrap())
    }
}

fn test_mc_basic(config: &TestConfig) -> TestResult {
    let mut sys = build_system(config);

    let procs = sys.process_names_sorted();

    let key = "ZXSA0H2K";
    let replicas = key_replicas(&key, &sys);
    let non_replicas = key_non_replicas(&key, &sys);

    println!("Key {} replicas: {:?}", key, replicas);
    println!("Key {} non-replicas: {:?}", key, non_replicas);
    sys.network().set_delay(0.0);

    let mut start_states = HashSet::new();
    // get key from the first node
    start_states = check_mc_get(&mut sys, &procs[0], &procs[0], &key, vec![], 2, 7, None, None)?.collected;
    println!("stage 1: {}", start_states.len());
    if start_states.is_empty() {
        return Err("stage 1 has no positive outcomes".to_owned());
    }

    start_states = HashSet::from_iter(vec![start_states.into_iter().next().unwrap()].into_iter());
    // put key from the first replica
    let value = "9ps2p1ua";
    start_states = check_mc_put(
        &mut sys,
        &replicas[0],
        &replicas[0],
        &key,
        &value,
        vec![&value],
        None,
        2,
        7,
        None,
        None,
    )?
    .collected;
    println!("stage 2: {}", start_states.len());
    start_states = get_n_start_states(start_states, 10);
    if start_states.is_empty() {
        return Err("stage 2 has no positive outcomes".to_owned());
    }

    start_states = get_n_start_states(start_states, 10);

    start_states = mc_stabilize(&mut sys, 15, Some(start_states))?.collected;
    println!("stage 3: {}", start_states.len());
    if start_states.is_empty() {
        return Err("stage 3 has no positive outcomes".to_owned());
    }

    // get key from the last replica
    start_states = check_mc_get(
        &mut sys,
        &replicas[2],
        &replicas[2],
        &key,
        vec![&value],
        2,
        15,
        Some(15),
        Some(start_states),
    )?
    .collected;
    println!("stage 4: {}", start_states.len());
    if start_states.is_empty() {
        return Err("stage 4 has no positive outcomes".to_owned());
    }
    Ok(true)
}

fn test_mc_sloppy_quorum_hinted_handoff(config: &TestConfig) -> TestResult {
    let mut sys = build_system(config);

    let procs = sys.process_names_sorted();

    let key = "ZXSA0H2K";
    let replicas = key_replicas(&key, &sys);
    let non_replicas = key_non_replicas(&key, &sys);

    println!("Key {} replicas: {:?}", key, replicas);
    println!("Key {} non-replicas: {:?}", key, non_replicas);

    sys.network()
        .make_partition(&[&replicas[0], &non_replicas[0]], &[&replicas[1], &replicas[2]]);
    sys.network().set_delay(0.0);

    let mut start_states = HashSet::new();
    // get key from the first node
    start_states = check_mc_get(&mut sys, &replicas[0], &replicas[0], &key, vec![], 2, 12, None, None)?.collected;
    println!("stage 1: {}", start_states.len());
    if start_states.is_empty() {
        return Err("stage 1 has no positive outcomes".to_owned());
    }
    // put key from the first replica
    let value = "9ps2p1ua";
    start_states = check_mc_put(
        &mut sys,
        &replicas[0],
        &replicas[0],
        &key,
        &value,
        vec![&value],
        None,
        2,
        12,
        None,
        None,
    )?
    .collected;
    println!("stage 2: {}", start_states.len());
    start_states = get_n_start_states(start_states, 10);
    if start_states.is_empty() {
        return Err("stage 2 has no positive outcomes".to_owned());
    }

    sys.network().reset_network();
    sys.network().set_delay(0.0);

    start_states = mc_stabilize(&mut sys, 15, Some(start_states))?.collected;
    println!("stage 3: {}", start_states.len());
    if start_states.is_empty() {
        return Err("stage 3 has no positive outcomes".to_owned());
    }

    start_states = get_n_start_states(start_states, 10);
    sys.network()
        .make_partition(&[&replicas[0], &non_replicas[0]], &[&replicas[1], &replicas[2]]);
    sys.network().set_delay(0.0);
    // get key from the last replica
    start_states = check_mc_get(
        &mut sys,
        &replicas[2],
        &replicas[2],
        &key,
        vec![&value],
        2,
        15,
        Some(15),
        Some(start_states),
    )?
    .collected;
    println!("stage 4: {}", start_states.len());
    if start_states.is_empty() {
        return Err("stage 4 has no positive outcomes".to_owned());
    }
    Ok(true)
}

fn test_mc_concurrent(config: &TestConfig) -> TestResult {
    let mut sys = build_system(config);

    let procs = sys.process_names_sorted();

    let key = "ZXSA0H2K";
    let replicas = key_replicas(&key, &sys);
    let non_replicas = key_non_replicas(&key, &sys);

    println!("Key {} replicas: {:?}", key, replicas);
    println!("Key {} non-replicas: {:?}", key, non_replicas);

    // put key to the first replica
    let value = "9ps2p1ua";
    let value2 = "8ab54uye";

    sys.network().disconnect_node(&replicas[0]);
    sys.network().disconnect_node(&replicas[1]);
    // just so we dont need to prune order for messages and timers
    sys.network().set_delay(0.0);
    sys.send_local_message(
        &replicas[0],
        Message::json(
            "PUT",
            &PutMessage {
                quorum: 1,
                key,
                value,
                context: None,
            },
        ),
    );

    let mut mc = ModelChecker::new(
        &sys,
        Box::new(Dfs::new(
            mc_query_prune(1, 6),
            mc_goal_query_finished(&replicas[0], &replicas[0]),
            mc_invariant_ok(),
            Some(mc_query_collect(&replicas[0], &replicas[0])),
            dslab_mp::mc::strategy::ExecutionMode::Debug,
        )),
    );
    let res = mc.run();
    if res.is_err() {
        return Err(format!("model checking found error {}", res.as_ref().err().unwrap()));
    }
    println!("{:?}", res.clone().unwrap().summary);
    mc = ModelChecker::new(
        &sys,
        Box::new(Dfs::new(
            mc_query_prune(1, 6),
            mc_goal_query_finished(&replicas[1], &replicas[1]),
            mc_invariant_ok(),
            Some(mc_query_collect(&replicas[1], &replicas[1])),
            dslab_mp::mc::strategy::ExecutionMode::Debug,
        )),
    );
    let mut start_states = res.unwrap().collected;
    println!("stage 1: {}", start_states.len());
    let mut after_put = McResult::default();
    for mut start_state in start_states {
        mc_clear_state_history(&mut start_state);
        mc.set_state(start_state);
        mc.apply_event(McEvent::LocalMessageReceived {
            msg: Message::json(
                "PUT",
                &PutMessage {
                    quorum: 1,
                    key,
                    value: value2,
                    context: None,
                },
            ),
            dest: replicas[1].clone(),
        });
        let res = mc.run();
        if res.is_err() {
            return Err(format!("model checking found error {}", res.as_ref().err().unwrap()));
        }
        after_put.combine(res.unwrap());
    }
    let mut start_states = after_put.collected;
    start_states = get_n_start_states(start_states, 10);
    if start_states.is_empty() {
        return Err("stage 2 has no positive outcomes".to_owned());
    }
    sys.network().reset_network();
    sys.network().set_drop_rate(0.0);
    sys.network().set_delay(0.0);
    let mut start_states_updated = HashSet::new();
    for mut start_state in start_states {
        while !start_state.events.available_events().is_empty() {
            start_state
                .events
                .pop(start_state.events.available_events().into_iter().next().unwrap());
        }
        start_states_updated.insert(start_state);
    }
    start_states = check_mc_get(
        &mut sys,
        &replicas[2],
        &replicas[2],
        &key,
        vec![&value, &value2],
        3,
        16,
        Some(16),
        Some(start_states_updated),
    )?
    .collected;
    println!("stage 2: {}", start_states.len());
    if start_states.is_empty() {
        return Err("stage 2 has no positive outcomes".to_owned());
    }
    Ok(true)
}

fn test_mc_concurrent_cart(config: &TestConfig) -> TestResult {
    let mut sys = build_system(config);

    let procs = sys.process_names_sorted();

    let key = "CART_ZXSA0H2K";
    let replicas = key_replicas(&key, &sys);
    let non_replicas = key_non_replicas(&key, &sys);

    println!("Key {} replicas: {:?}", key, replicas);
    println!("Key {} non-replicas: {:?}", key, non_replicas);

    // put key to the first replica
    let value = "a,b,c";
    let value2 = "b,c,d";

    sys.network().disconnect_node(&replicas[0]);
    sys.network().disconnect_node(&replicas[1]);
    // just so we dont need to prune order for messages and timers
    sys.network().set_delay(0.0);
    sys.send_local_message(
        &replicas[0],
        Message::json(
            "PUT",
            &PutMessage {
                quorum: 1,
                key,
                value,
                context: None,
            },
        ),
    );

    let mut mc = ModelChecker::new(
        &sys,
        Box::new(Dfs::new(
            mc_query_prune(1, 6),
            mc_goal_query_finished(&replicas[0], &replicas[0]),
            mc_invariant_ok(),
            Some(mc_query_collect(&replicas[0], &replicas[0])),
            dslab_mp::mc::strategy::ExecutionMode::Debug,
        )),
    );
    let res = mc.run();
    if res.is_err() {
        return Err(format!("model checking found error {}", res.as_ref().err().unwrap()));
    }
    println!("{:?}", res.clone().unwrap().summary);
    let mut start_states = res.unwrap().collected;
    println!("stage one: {}", start_states.len());
    let mut after_put = McResult::default();
    mc = ModelChecker::new(
        &sys,
        Box::new(Dfs::new(
            mc_query_prune(1, 6),
            mc_goal_query_finished(&replicas[1], &replicas[1]),
            mc_invariant_ok(),
            Some(mc_query_collect(&replicas[1], &replicas[1])),
            dslab_mp::mc::strategy::ExecutionMode::Debug,
        )),
    );
    for mut start_state in start_states {
        mc_clear_state_history(&mut start_state);
        mc.set_state(start_state);
        mc.apply_event(McEvent::LocalMessageReceived {
            msg: Message::json(
                "PUT",
                &PutMessage {
                    quorum: 1,
                    key,
                    value: value2,
                    context: None,
                },
            ),
            dest: replicas[1].clone(),
        });
        let res = mc.run();
        if res.is_err() {
            return Err(format!("model checking found error {}", res.as_ref().err().unwrap()));
        }
        after_put.combine(res.unwrap());
    }
    println!("{:?}", after_put.summary);

    let mut start_states = after_put.collected;
    start_states = get_n_start_states(start_states, 10);
    if start_states.is_empty() {
        return Err("stage 1 has no positive outcomes".to_owned());
    }

    sys.network().reset_network();
    sys.network().set_drop_rate(0.0);
    sys.network().set_delay(0.0);
    let mut start_states_updated = HashSet::new();
    for mut start_state in start_states {
        while !start_state.events.available_events().is_empty() {
            start_state
                .events
                .pop(start_state.events.available_events().into_iter().next().unwrap());
        }
        start_states_updated.insert(start_state);
    }
    start_states = check_mc_get(
        &mut sys,
        &replicas[2],
        &replicas[2],
        &key,
        vec!["a,b,c,d"],
        3,
        12,
        Some(12),
        Some(start_states_updated),
    )?
    .collected;
    println!("stage 2: {}", start_states.len());
    if start_states.is_empty() {
        return Err("stage 2 has no positive outcomes".to_owned());
    }
    Ok(true)
}


fn test_mc_concurrent_xcart(config: &TestConfig) -> TestResult {
    let mut sys = build_system(config);
    let mut rand = Pcg64::seed_from_u64(config.seed);

    let key = format!("xcart-{}", random_string(8, &mut rand)).to_uppercase();
    let replicas = key_replicas(&key, &sys);    

    sys.network().set_delay(0.0);
    let mut start_states = check_mc_put(&mut sys, &replicas[2], &replicas[2], &key, "a,b", vec!["a,b"], None, 3, 12, Some(12), None)?.collected;
    start_states = mc_stabilize(&mut sys, 15, Some(start_states))?.collected;
    // start_states = start_states.into_iter().filter(|state| state.events.available_events_num() == 0).collect::<HashSet<McState>>();
    sys.network().disconnect_node(&replicas[0]);
    sys.network().disconnect_node(&replicas[1]);

    for start_state in start_states {
        let msg = &start_state.node_states[&replicas[2]][&replicas[2]].local_outbox[0];
        let data: PutRespMessage = serde_json::from_str(&msg.data).map_err(|err| err.to_string())?;
        let ctx = data.context.as_ref();
        println!("CONTEXT {}", ctx);
        let mut new_start_states = check_mc_put(&mut sys, &replicas[0], &replicas[0], &key, "a,b,c", vec!["a,b,c"], Some(ctx.to_string()), 1, 8, Some(8), Some(HashSet::from_iter(vec![start_state.clone()])))?.collected;
        new_start_states = mc_stabilize(&mut sys, 4, Some(new_start_states))?.collected;
        new_start_states = get_n_start_states(new_start_states, 10);
        let mut after_add_states = HashSet::new();
        for new_start_state in new_start_states {
            let mut updated_state = new_start_state.clone();
            for event_id in new_start_state.events.available_events() {
                if let Some(McEvent::MessageDropped { .. }) = new_start_state.events.get(event_id) {
                    updated_state.events.pop(event_id);
                }
                if let Some(McEvent::MessageReceived { .. }) = new_start_state.events.get(event_id) {
                    updated_state.events.pop(event_id);
                }
            }
            after_add_states.insert(updated_state);
        }
        after_add_states = get_n_start_states(after_add_states, 10);
        new_start_states = check_mc_put(&mut sys, &replicas[1], &replicas[1], &key, "d", vec!["d"], Some(ctx.to_string()), 1, 8, Some(8), Some(after_add_states))?.collected;
        new_start_states = get_n_start_states(new_start_states, 10);
        sys.network().reset_network();
        sys.network().set_delay(0.0);
        check_mc_get(&mut sys, &replicas[2], &replicas[2], &key, vec!["c,d"], 3, 12, Some(12), Some(new_start_states))?;    
    }
    Ok(true)
}

// CLI -----------------------------------------------------------------------------------------------------------------

/// Replicated KV Store v2 Homework Tests
#[derive(Parser, Debug)]
#[clap(about, long_about = None)]
struct Args {
    /// Path to Python file with solution
    #[clap(long = "impl", short = 'i', default_value = "../python/solution.py")]
    solution_path: String,

    /// Test to run (optional)
    #[clap(long = "test", short)]
    test: Option<String>,

    /// Print execution trace
    #[clap(long, short)]
    debug: bool,

    /// Number of processes used in tests
    #[clap(long, short, default_value = "6")]
    process_count: u32,

    /// Random seed used in tests
    #[clap(long, short, default_value = "123")]
    seed: u64,
}

// MAIN --------------------------------------------------------------------------------------------

fn main() {
    let args = Args::parse();
    if args.debug {
        init_logger(LevelFilter::Trace);
    }
    env::set_var("PYTHONPATH", "../../crates/dslab-mp-python/python");
    env::set_var("PYTHONHASHSEED", args.seed.to_string());
    let process_factory = PyProcessFactory::new(&args.solution_path, "StorageProcess");
    let config = TestConfig {
        process_factory: &process_factory,
        process_count: args.process_count,
        seed: args.seed,
    };

    let mut tests = TestSuite::new();
    tests.add("BASIC", test_basic, config);
    tests.add("STALE REPLICA", test_stale_replica, config);
    tests.add("CONCURRENT WRITES 1", test_concurrent_writes_1, config);
    tests.add("CONCURRENT WRITES 2", test_concurrent_writes_2, config);
    tests.add("CONCURRENT WRITES 3", test_concurrent_writes_3, config);
    tests.add("DIVERGED REPLICAS", test_diverged_replicas, config);
    tests.add("SLOPPY QUORUM", test_sloppy_quorum, config);
    tests.add("PARTITIONED CLIENTS", test_partitioned_clients, config);
    tests.add("SHOPPING CART 1", test_shopping_cart_1, config);
    tests.add("SHOPPING CART 2", test_shopping_cart_2, config);
    tests.add("SHOPPING XCART 1", test_shopping_xcart_1, config);
    tests.add("SHOPPING XCART 2", test_shopping_xcart_2, config);
    let mc_config = TestConfig {
        process_factory: &process_factory,
        process_count: 4,
        seed: args.seed,
    };
    tests.add("MODEL CHECKING NORMAL", test_mc_basic, mc_config);
    tests.add(
        "MODEL CHECKING SLOPPY QUORUM",
        test_mc_sloppy_quorum_hinted_handoff,
        mc_config,
    );
    tests.add("MODEL CHECKING CONCURRENT", test_mc_concurrent, mc_config);
    tests.add("MODEL CHECKING CONCURRENT CART", test_mc_concurrent_cart, mc_config);
    tests.add("MODEL CHECKING CONCURRENT XCART", test_mc_concurrent_xcart, mc_config);

    if args.test.is_none() {
        tests.run();
    } else {
        tests.run_test(&args.test.unwrap());
    }
}
