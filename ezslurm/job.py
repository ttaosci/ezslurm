import subprocess
import time
from abc import ABC, abstractmethod
from enum import Enum

from ezslurm.config import SlurmConfig
from ezslurm.utils import get_logger, setup_logger

logger = get_logger()


class Status(Enum):
    INIT = "init"
    RUNNING = "running"
    DONE = "done"


class Job(ABC):
    def __init__(self, command, job_id):
        if isinstance(command, list) and isinstance(command[0], str):
            command = f"bash -c \"{'; '.join(command)}\""
        self.command = command
        self.job_id = job_id
        self.status = Status.INIT
        self.stdout = ""
        self.stderr = ""

    @abstractmethod
    def submit(self):
        # Submit a job and change the status to RUNNING
        raise NotImplementedError("submit() is not implemented")

    @abstractmethod
    def update_status(self):
        # Update the status of the job
        raise NotImplementedError("update_status() is not implemented")


class ForegroundJob(Job):
    def __init__(self, command, job_id, **kwargs):
        super().__init__(command, job_id)

    def submit(self):
        self.status = Status.RUNNING
        result = subprocess.run(
            self.command,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        self.stdout = result.stdout.decode("utf-8")
        self.stderr = result.stderr.decode("utf-8")
        self.status = Status.DONE

    def update_status(self):
        pass


class BackgroundJob(Job):
    def __init__(self, command, job_id, **kwargs):
        super().__init__(command, job_id)
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
                logger.warning("Failed to decode stdout/stderr")


class SlurmJob(ForegroundJob):
    def __init__(self, command, job_id, slurm_config):
        super().__init__(command, job_id)
        self.slurm_config = slurm_config
        self.command = self.slurm_config.command(
            self.command,
            sbatch=True,
        )

        # The slurm job ID assigned by slurm during submit()
        self.slurm_job_id = None

    def submit(self):
        super().submit()
        self.status = Status.RUNNING
        # E.g., "Submitted batch job 6449881"
        assert "Submitted batch job" in self.stdout
        self.slurm_job_id = int(self.stdout.split(" ")[3])

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


class SequentialJob(Job):
    def __init__(self, command, job_id, **kwargs):
        # In sequential mode, command should be an iterable of commands
        super().__init__(command, job_id)
        self.slurm_config = kwargs.get("slurm_config", None)
        self.job_cls = (
            SlurmJob if self.slurm_config is not None else BackgroundJob
        )

        self.stdout = []
        self.stderr = []

        # Current job that is running
        self.curr_job = None

    def get_next_job(self):
        return self.job_cls(
            next(self.command), self.job_id, slurm_config=self.slurm_config
        )

    def submit(self):
        self.curr_job = self.get_next_job()
        self.curr_job.submit()
        self.status = Status.RUNNING

    def update_status(self):
        self.curr_job.update_status()
        if self.curr_job.status == Status.DONE:
            self.stdout.append(self.curr_job.stdout)
            self.stderr.append(self.curr_job.stderr)
            try:
                self.curr_job = self.get_next_job()
                self.curr_job.submit()
            except StopIteration:
                self.status = Status.DONE


class JobManager:
    JOB_CLASSES = {
        "bash": BackgroundJob,
        "slurm": SlurmJob,
        "sequential": SequentialJob,
    }

    def __init__(self, max_concurrent_jobs, wait_sec=10):
        self.max_concurrent_jobs = max_concurrent_jobs
        self.wait_sec = wait_sec

        self.todo_list = []
        self.active_list = []
        self.done_list = []

    def add_job(self, command, mode="bash", slurm_config=None):
        job_id = len(self.todo_list)
        job = self.JOB_CLASSES[mode](
            command, job_id, slurm_config=slurm_config
        )
        self.todo_list.append(job)

    def run_once(self):
        progress_msg = ""
        while len(self.todo_list) > 0 or len(self.active_list) > 0:
            if progress_msg != self.progress_msg:
                progress_msg = self.progress_msg
                logger.info(progress_msg)
            self.assign_jobs()
            time.sleep(self.wait_sec)
            self.update_jobs()

    def run(self, num_run=1):
        for _ in range(num_run):
            self.run_once()

    def assign_jobs(self):
        while (
            len(self.todo_list) > 0
            and len(self.active_list) < self.max_concurrent_jobs
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
                logger.debug(f"Job-{job.job_id} stdout: {job.stdout}")
                logger.debug(f"Job-{job.job_id} stderr: {job.stderr}")
                done_list.append(job)
        for job in done_list:
            self.active_list.remove(job)
        self.done_list.extend(done_list)

    @property
    def progress_msg(self):
        return f"todo/active/done jobs: {len(self.todo_list)}/{len(self.active_list)}/{len(self.done_list)}"


if __name__ == "__main__":
    import logging
    import random

    from ezslurm.utils import setup_logger

    setup_logger(stream_level=logging.DEBUG)

    manager = JobManager(max_concurrent_jobs=2)

    test_sequential = False
    if test_sequential:

        def command_gen(begin, end):
            for i in range(begin, end):
                yield f"echo 'Executing Task {i + 1}' && sleep {random.randint(1, 10)}"

        manager.add_job(
            command_gen(0, 3),
            mode="sequential",
            # slurm_config=SlurmConfig(job_name=f"task_{i}"),
        )
        manager.add_job(
            command_gen(3, 6),
            mode="sequential",
            # slurm_config=SlurmConfig(job_name=f"task_{i}"),
        )
    else:
        # Add sample jobs (replace with your actual shell commands)
        for i in range(3):
            command = f"echo 'Executing Task {i + 1}' && sleep {random.randint(1, 10)}"
            if i % 3 == 0:
                # Randomly fail some jobs
                command += " && printf 'failed :(' >&2 && exit 1"
            manager.add_job(
                command,
                mode="slurm",
                slurm_config=SlurmConfig(job_name=f"task_{i}"),
            )

    # Run the job manager
    manager.run(num_run=1)
