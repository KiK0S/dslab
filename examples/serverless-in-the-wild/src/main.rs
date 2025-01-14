mod plot;

use std::boxed::Box;
use std::fs::File;
use std::io::Write;
use std::path::Path;

use clap::Parser;

use serde::{Deserialize, Serialize};

use dslab_faas::coldstart::{ColdStartPolicy, FixedTimeColdStartPolicy};
use dslab_faas::config::{ConfigParamResolvers, RawConfig};
use dslab_faas::extra::azure_trace::{process_azure_trace, AppPreference, AzureTraceConfig};
use dslab_faas::extra::hybrid_histogram::HybridHistogramPolicy;
use dslab_faas::parallel::parallel_simulation_raw;
use dslab_faas::stats::SampleMetric;

use crate::plot::plot_results;

#[derive(Serialize, Deserialize)]
struct ExperimentConfig {
    pub base_config: RawConfig,
    pub coldstart_policies: Vec<String>,
}

fn policy_resolver(s: &str) -> Box<dyn ColdStartPolicy> {
    match &s[s.len() - 9..] {
        "keepalive" => {
            let s1 = s.split('-').next().unwrap();
            let len = s1.parse::<f64>().unwrap();
            Box::new(FixedTimeColdStartPolicy::new(len * 60.0, 0.0))
        }
        "unloading" => Box::new(FixedTimeColdStartPolicy::new(f64::MAX / 10.0, 0.0)),
        _ => {
            let mut it = s.split(',');
            it.next();
            let s1 = it.next().unwrap();
            let s2 = s1[1..].split(' ').next().unwrap();
            let len = s2.parse::<f64>().unwrap();
            Box::new(HybridHistogramPolicy::new(3600.0 * len, 60.0, 2.0, 0.5, 0.15, 0.1))
        }
    }
}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Path to a directory with Azure Functions trace.
    trace: String,
    /// Path to a simulation config in YAML format.
    #[arg(long)]
    config: String,
    /// Plot output path (if needed).
    #[arg(long)]
    plot: Option<String>,
    /// Dump final metrics to given file.
    #[arg(long)]
    dump: Option<String>,
}

fn main() {
    let args = Args::parse();
    let trace_config = AzureTraceConfig {
        time_period: 8 * 60,
        app_preferences: vec![AppPreference::new(68, 0.45, 0.55)],
        concurrency_level: 16,
        ..Default::default()
    };
    let trace = Box::new(process_azure_trace(Path::new(&args.trace), trace_config));
    println!(
        "trace processed successfully, {} invocations",
        trace.trace_records.len()
    );
    let experiment_config: ExperimentConfig =
        serde_yaml::from_reader(File::open(Path::new(&args.config)).unwrap()).unwrap();
    let policies = experiment_config.coldstart_policies;
    let base_config = experiment_config.base_config;
    let configs: Vec<_> = policies
        .iter()
        .map(|x| {
            let mut config = base_config.clone();
            config.coldstart_policy = x.to_string();
            config
        })
        .collect();
    let resolvers = ConfigParamResolvers {
        coldstart_policy_resolver: Box::new(policy_resolver),
        ..Default::default()
    };
    let mut stats = parallel_simulation_raw(configs, resolvers, vec![trace], vec![1]);
    let mut results = Vec::with_capacity(stats.len());
    for (i, s) in stats.drain(..).enumerate() {
        s.global_stats.print_summary(&policies[i]);
        if args.plot.is_some() {
            let mut apps: SampleMetric = Default::default();
            for app_stats in s.app_stats.iter() {
                apps.add((app_stats.cold_starts as f64) / (app_stats.invocations as f64) * 100.);
            }
            results.push((
                apps.quantile(0.75),
                s.global_stats.wasted_resource_time.get(0).unwrap().sum(),
            ));
        }
    }
    if let Some(s) = args.dump {
        let mut out = File::create(s).unwrap();
        writeln!(&mut out, "policy,75% app coldstart frequency,wasted memory time").unwrap();
        for (policy, result) in std::iter::zip(policies.iter(), results.iter()) {
            writeln!(&mut out, "{},{:.4},{:.4}", policy, result.0, result.1).unwrap();
        }
    }
    if let Some(plot) = args.plot {
        let mut pos = usize::MAX;
        for (i, p) in policies.iter().enumerate() {
            if p.contains("10-minute keepalive") {
                pos = i;
                break;
            }
        }
        assert!(pos != usize::MAX);
        let base = results[pos].1;
        for p in results.iter_mut() {
            p.1 = p.1 / base * 100.;
        }
        plot_results(&plot, policies, results);
    }
}
