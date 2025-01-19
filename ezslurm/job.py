import logging
import subprocess
import time
from enum import Enum

from ezslurm.config import SlurmConfig

logger = logging.getLogger(__name__)


DEFAULT_SLURM_CONFIG = SlurmConfig(
    job_name="default",
    output=None,
    error=None,
    nodes=1,
    ntasks_per_node=1,
    cpus_per_task=1,
    mem="16g",
    time="1-00:00:00",
    partition=None,
    gres=None,
    export=None,
    exclude=None,
    nodelist=None,
)


class Status(Enum):
    INIT = "init"
    RUNNING = "running"
    DONE = "done"


class Job:
    """Job class for normal bash job"""

    def __init__(self, command, job_id, **kwargs):
        if isinstance(command, list):
            command = f"bash -c \"{'; '.join(command)}\""
        self.command = command
        self.job_id = job_id
        self.status = Status.INIT
        self.process = None

    def submit(self):
        self.process = subprocess.Popen(
            self.command,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        self.status = Status.RUNNING

    def update_status(self):
        if self.process is not None and self.process.poll() is not None:
            # Process ends
            self.status = Status.DONE
            stdout, stderr = self.process.communicate()
            try:
                self.stdout, self.stderr = (
                    stdout.decode("utf-8"),
                    stderr.decode("utf-8"),
                )
            except UnicodeDecodeError:
                self.stdout, self.stderr = "", ""


class SlurmRunJob(Job):
    """Job class for srun job"""

    def __init__(self, command, job_id, slurm_config=None):
        super().__init__(command, job_id)

        if slurm_config is None:
            logger.warning("Slurm config is not provided. Use default config.")
            slurm_config = DEFAULT_SLURM_CONFIG

        self.slurm_config = slurm_config
        self.command = slurm_config.command(
            self.command,
            sbatch=False,
        )


class SlurmBatchJob(Job):
    """Job class for sbatch job"""

    def __init__(self, command, job_id, slurm_config=None):
        super().__init__(command, job_id)

        if slurm_config is None:
            logger.warning("Slurm config is not provided. Use default config.")
            slurm_config = DEFAULT_SLURM_CONFIG

        self.slurm_config = slurm_config
        self.command = self.slurm_config.command(
            self.command,
            sbatch=True,
        )

        # The job ID assigned by slurm
        self.slurm_job_id = None

    def submit(self):
        result = subprocess.run(
            self.command,
            shell=True,
            stdout=subprocess.PIPE,
        )
        # E.g., "Submitted batch job 6449881"
        stdout = result.stdout.decode("utf-8")
        assert "Submitted batch job" in stdout
        self.slurm_job_id = int(stdout.split(" ")[3])
        self.status = Status.RUNNING

    def update_status(self):
        if self.slurm_job_id is not None:
            result = subprocess.run(
                "squeue --me -o %i --noheader",
                shell=True,
                stdout=subprocess.PIPE,
            )
            stdout = result.stdout.decode("utf-8")

            active_jobs = [
                int(job_id.strip()) for job_id in stdout.split("\n") if job_id
            ]

            if self.slurm_job_id not in active_jobs:
                self.status = Status.DONE


class JobManager:
    def __init__(self, max_run_jobs, wait_sec=10):
        self.max_run_jobs = max_run_jobs
        self.wait_sec = wait_sec

        self.todo_list = []
        self.active_list = []
        self.done_list = []

    def add_job(self, command, mode="bash", slurm_config=None):
        job_id = len(self.todo_list)
        if mode == "bash":
            job = Job(command, job_id)
        elif mode == "srun":
            job = SlurmRunJob(command, job_id, slurm_config=slurm_config)
        elif mode == "sbatch":
            job = SlurmBatchJob(command, job_id, slurm_config=slurm_config)
        else:
            raise NotImplementedError(f"Invalid mode: {mode}")
        self.todo_list.append(job)

    def run_once(self):
        progress = None
        while len(self.todo_list) > 0 or len(self.active_list) > 0:
            if progress != self.progress:
                progress = self.progress
                logger.info(progress)
            self.assign_jobs()
            time.sleep(self.wait_sec)
            self.update_jobs()

    def run(self, num_run=1):
        for _ in range(num_run):
            self.run_once()

    def assign_jobs(self):
        while (
            len(self.todo_list) > 0
            and len(self.active_list) < self.max_run_jobs
        ):
            job = self.todo_list.pop(0)
            job.submit()
            logger.info(f"Submit job-{job.job_id}: {job.command}")
            self.active_list.append(job)

    def update_jobs(self):
        done_list = []
        for job in self.active_list:
            job.update_status()
            if job.status == Status.DONE:
                logger.info(f"Job-{job.job_id} done")
                done_list.append(job)
        for job in done_list:
            self.active_list.remove(job)
        self.done_list.extend(done_list)

    @property
    def progress(self):
        return f"todo/active/done jobs: {len(self.todo_list)}/{len(self.active_list)}/{len(self.done_list)}"


if __name__ == "__main__":
    import random

    from ezslurm.utils import setup_logger

    setup_logger()

    manager = JobManager(max_run_jobs=2)

    # Add sample jobs (replace with your actual shell commands)
    for i in range(3):
        command = (
            f"echo 'Executing Task {i + 1}' && sleep {random.randint(1, 10)}"
        )
        if i % 3 == 0:
            # Randomly fail some jobs
            command += " && printf 'failed :(' >&2 && exit 1"
        manager.add_job(
            command,
            mode="srun",
            slurm_config=SlurmConfig(job_name=f"task_{i}"),
        )

    # Run the job manager
    manager.run(num_run=1)
