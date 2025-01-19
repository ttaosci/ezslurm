<h1 align="center">Easy Slurm</h1>
<p align="center">An easy python wrapper for SLURM job submission<p>

```python
import ezslurm

# Slurm configuration using python
cfg = ezslurm.SlurmConfig(job_name="ezslurm_demo", nodes=1, time="0-00:10:00")

# Easy to create slurm command
srun_cmd = cfg.command(["echo hello", "sleep 5"], sbatch=False)
sbatch_cmd = cfg.command(["echo hello", "sleep 5"], sbatch=True)

# Job manager for slurm job submission
manager = ezslurm.JobManager(max_concurrent_jobs=10)
# Currently, ezslurm supports three types of jobs: (1) slurm (2) bash (3) sequential
manager.add_job(["echo 'add job-0'", "sleep 5"], mode="slurm", slurm_config=cfg)
manager.add_job(["echo 'add job-1'", "sleep 5"], mode="bash")
# Sequential job (many jobs that need to be run in order)
def command_iter(begin, end):
  import random
  for i in range(begin, end):
    yield f"echo 'Executing Task {i}' && sleep {random.randint(1, 10)}"
manager.add_job(command_iter(0, 3), mode="sequential")

# Start running jobs
manager.run()
```

