import subprocess
import json
import os
import time
import re
import signal

# Load config from JSON file
with open('config.json', 'r') as f:
    config = json.load(f)

# Create output directory if it doesn't exist
if not os.path.exists(config['output_dir']):
    os.makedirs(config['output_dir'])

# Initialize summary
summary = {
    'total': 0,
    'success': [],
    'wrong': [],
    'non_zero_exit': [],
    'timed_out': [],
    'reports': {},
    'test_results': {}
}

# Run command with each argument
for arg in config['args']:
    # Construct command
    cmd = config['command'] + [arg]

    # Run command in subprocess and redirect stdout and stderr to separate log files
    stdout_file = os.path.join(config['output_dir'], arg + '_stdout.log')
    stderr_file = os.path.join(config['output_dir'], arg + '_stderr.log')
    print(cmd)
    with open(stdout_file, 'w') as stdout, open(stderr_file, 'w') as stderr:
        p = subprocess.Popen(cmd, stdout=stdout, stderr=stderr, preexec_fn=os.setsid)

    # Wait for process to finish or time out
    start_time = time.time()
    while True:
        if p.poll() is not None:
            # Process has finished
            break
        elif time.time() - start_time > config['timeout']:
            print(time.time(), start_time)
            # Process has timed out
            os.killpg(os.getpgid(p.pid), signal.SIGTERM) 
            summary['timed_out'].append(arg)
            break
        else:
            time.sleep(1)

    # Check exit code
    if p.returncode == 0:
        summary['success'].append(arg)
    elif p.returncode == 1:
        summary['wrong'].append(arg)
    else:
        summary['non_zero_exit'].append(arg)

    # Write log file
    log_file = os.path.join(config['output_dir'], arg + '.log')
    with open(log_file, 'w') as f:
        f.write(f"Command: {' '.join(cmd)}\n")
        f.write(f"Exit code: {p.returncode}\n")
        f.write(f"Time taken: {time.time() - start_time:.2f} seconds\n")
        f.write(f"Stdout: {stdout_file}\n")
        f.write(f"Stderr: {stderr_file}\n")

    # Get summary for tests:
    summary['reports'][arg] = {}

    with open(stdout_file, 'r') as f:
        cur_test = None
        for line in f.readlines():
            m = re.match(r'--- (.+[^-]) ---', line)
            if m:
                cur_test = str(m.group(1))
            m = re.match(r'((PASSED)|(FAILED))', line)
            if m:
                status = m.group(1)
                results = summary['test_results'].get(cur_test, {})
                sols = results.get(status, [])
                sols.append(arg)
                results[status] = sols
                summary['test_results'][cur_test] = results
                summary['reports'][arg][cur_test] = status
                cur_test = None
        if cur_test is not None:
            status = 'RUNTIME_ERROR'
            if arg in summary['timed_out']:
                status = 'TIME_LIMIT'
            results = summary['test_results'].get(cur_test, {})
            sols = results.get(status, [])
            sols.append(arg)
            results[status] = sols
            summary['test_results'][cur_test] = results
            summary['reports'][arg][cur_test] = status

    # Increment total count
    summary['total'] += 1

# Write summary file
summary_file = os.path.join(config['output_dir'], 'summary.txt')
with open(summary_file, 'w') as f:
    f.write(f"Total runs: {summary['total']}\n")
    f.write(f"Successful runs ({len(summary['success'])}): {summary['success']}\n")
    f.write(f"Wrong runs ({len(summary['wrong'])}): {summary['wrong']}\n")
    f.write(f"Failed runs (non-zero exit code) ({len(summary['non_zero_exit'])}): {summary['non_zero_exit']}\n")
    f.write(f"Failed runs (timed out) ({len(summary['timed_out'])}): {summary['timed_out']}\n")
    f.write(f"\n\n{json.dumps(summary, indent=4, sort_keys=True)}\n")

