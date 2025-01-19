# Author: Tao Tu (tt582@cornell.edu)
import copy

import omegaconf


def format_key(k):
    # Change key format
    # E.g., cpus_per_task -> cpus-per-task
    return k.strip().replace("_", "-")


class SlurmConfig:
    """Slurm configuration class

    This class assumes the passed arguments are valid slurm options.

    Examples:
        >>> cfg = SlurmConfig(ntasks_per_node=1, cpus_per_task=4)
        >>> srun_cmd = cfg.command(["echo hello", "sleep 5"], sbatch=False)
        >>> sbatch_cmd = cfg.command(["echo hello", "sleep 5"], sbatch=True)
        >>> sbatch_cmd = cfg.command(["echo hello", "sleep 5"], sbatch=True, fpath="tmp.slurm")
    """

    def __init__(self, **kwargs):
        self.kwargs = kwargs

    def update(self, **kwargs):
        self.kwargs.update(kwargs)

    def new(self, **kwargs):
        new_cfg = copy.deepcopy(self)
        new_cfg.update(**kwargs)
        return new_cfg

    def __repr__(self):
        key_vals = [f"{k}={v}" for k, v in self.kwargs.items()]
        repr = f"{self.__class__.__name__}("
        repr += ", ".join(key_vals)
        repr += ")"
        return repr

    def command(self, core_cmd, sbatch=False, fpath=None):
        # Merge commands to a single command
        core_cmd = (
            f"bash -c \"{'; '.join(core_cmd)}\""
            if isinstance(core_cmd, list)
            else core_cmd
        )
        if sbatch:
            sbatch_script = f"#!/bin/bash"
            for k, v in self.kwargs.items():
                sbatch_script += f"\n#SBATCH --{format_key(k)}={v}"
            sbatch_script += f"\n\n{core_cmd}"

            cmd = "sbatch"
            if fpath is None:
                cmd += f" << EOF\n{sbatch_script}\nEOF"
            else:
                with open(fpath, "w") as f:
                    f.write(sbatch_script)
                cmd += f" {fpath}"
        else:
            cmd = "srun"
            for k, v in self.kwargs.items():
                cmd += f" --{format_key(k)}={v}"
            cmd += f" {core_cmd}"
        return cmd

    @classmethod
    def from_yaml(cls, fpath):
        return cls(**omegaconf.OmegaConf.load(fpath))


if __name__ == "__main__":
    cfg = SlurmConfig(ntasks_per_node=1, cpus_per_task=4)
    print(cfg)

    commands = ["echo hello", "sleep 5"]
    print(cfg.command(commands, sbatch=False))
    print(cfg.command(commands, sbatch=True))
    print(cfg.command(commands, sbatch=True, fpath="tmp.slurm"))

    yaml_path = "config/gpu.yaml"
    cfg = SlurmConfig.from_yaml(yaml_path)
    print(cfg)

    cfg_new = cfg.new(job_name="new_job")
    print(cfg_new)
    print(cfg)
