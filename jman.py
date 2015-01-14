import json
import argparse
import sys
import subprocess
import os

LOG_DIR="/lustre/scratch113/teams/durbin/users/ss27/jman_out"

def wrap_cmd_args(cmd_args):
    wrapped_args = ["\"{}\"".format(a) if " " in a else a for a in cmd_args]
    return " ".join(wrapped_args)

taskgroups = []

class Task:
    def __init__(self, job_name, cmd, mem=100, queue="normal", farm_group=os.environ["LSB_DEFAULTGROUP"],
                 threads=1, my_project="default_project", job_group="default_group"):
        self.job_name = job_name
        self.cmd = cmd
        self.mem = mem
        self.queue = queue
        self.farm_group = farm_group
        self.my_project = my_project
        self.threads = threads
        self.job_group = job_group
        outdir = "{}/{}/{}/{}".format(LOG_DIR, self.farm_group, self.my_project, self.job_group)
        if not os.path.isdir(outdir):
            os.makedirs(outdir)
        self.outfile = "{}/{}.out".format(outdir, job_name)
    
    def __repr__(self):
        return "<jman.Task: {}>".format(self.job_name)
    
    def print_cmd(self):
        print(self.cmd)
    
    def cat(self):
        f = open(self.outfile, "r")
        for line in f:
            print(line, end="")
        
    def submit(self, test=False):
        R = "select[mem>{0}] rusage[mem={0}] span[hosts=1]".format(self.mem)
        cmd_args = ['bsub', '-R', R, '-M', str(self.mem), '-n', str(self.threads), '-q', self.queue, '-G', self.farm_group, '-g', "/" + self.job_group, '-J', self.job_name, '-oo', self.outfile, self.cmd]
        if test:
          print(wrap_cmd_args(cmd_args))
        else:
          subprocess.call(cmd_args)
    
    def check(self, quiet=False):
        if not os.path.exists(self.outfile):
            return "NOT RUN"
        result = subprocess.check_output(["tail", "-100", self.outfile], universal_newlines=True)
        ret = "NOT FINISHED"
        if "Resource usage summary:" in result:
            if "Successfully completed." in result:
                ret = "SUCCESS"
            elif "TERM_MEMLIMIT" in result:
                ret = "FAILED MEM"
            elif "TERM_RUNLIMIT" in result:
                ret = "FAILED RUNTIME"
            else:
                ret = "FAILED UNKNOWN"
        if not quiet:
            print("{}:\t{}".format(ret, self.outfile))
        return ret
    
    def status(self, quiet=False):
        bjobs_out = subprocess.check_output(["bjobs", "-J", self.job_name], stderr=subprocess.STDOUT)
        lines = bjobs_out.decode().strip().split("\n")
        if len(lines) > 1:
            fields = lines[1].split()
            if not quiet:
                print(lines[1])
            if fields[2] == "RUN":
                return True
        else:
            if not quiet:
                print(lines[0])
        return False
    
    def clean(self):
        if os.path.exists(self.outfile):
            os.remove(self.outfile)
    
    def kill(self, test=False):
        cmd_args = ["bkill", "-J", self.job_name]
        if test:
          print(wrap_cmd_args(cmd_args))
        else:
          subprocess.call(cmd_args)
        

class TaskGroup:
    def __init__(self, job_group="default_group", my_project="default_project", mem=100, queue="normal", farm_group=os.environ["LSB_DEFAULTGROUP"], threads=1, 
                 is_array=False):
        assert len(job_group) > 0, "need valid job group name"
        self.job_group = job_group
        self.my_project = my_project
        self.mem = mem
        self.queue = queue
        self.farm_group = farm_group
        self.threads = threads
        self.is_array = is_array
        self.outdir = "{}/{}/{}/{}".format(LOG_DIR, self.farm_group, self.my_project, self.job_group)
        if not os.path.isdir(self.outdir):
            os.makedirs(self.outdir)
        self.tasks = []
        taskgroups.append(self)
    
    def __repr__(self):
        return "<jman.TaskGroup: {}>".format(self.job_group)

    def append(self, cmd, job_name=None, mem=None, threads=None, queue=None):
        if job_name is None or self.is_array:
            job_name = "{}_{}".format(self.job_group, len(self.tasks) + 1)
        if mem is None:
            mem = self.mem
        if threads is None:
            threads = self.threads
        if queue is None:
            queue = self.queue
        task = Task(job_name, cmd, mem=mem, threads=threads, queue=queue, farm_group=self.farm_group, 
                    my_project=self.my_project, job_group=self.job_group)
        self.tasks.append(task)
    
    def print_cmd(self):
        for task in self.tasks:
            task.print_cmd()
    
    def submit(self, test=False, skip_succeeded=False):
        if self.is_array:
            self.submit_array(test, skip_succeeded)
        else:
            for task in self.tasks:
                if skip_succeeded:
                    if not task.check(True) == "SUCCESS":
                        task.submit(test)
                else:
                    task.submit(test)
    
    def check(self, quiet=False):
        check_counts = {}
        for task in self.tasks:
            c = task.check(quiet)
            if c not in check_counts:
                check_counts[c] = 0
            check_counts[c] += 1
        print("Job Group: {}. Nr Jobs: {}. Counts: {}".format(self.job_group, len(self.tasks), check_counts))
    
    def status(self, quiet=False):
        if self.is_array:
            self.status_array(quiet)
        else:
            run = 0
            for task in self.tasks:
                run += task.status(quiet)
            print("{} of {} running: {}".format(run, len(self.tasks), self.job_group))
        
    def clean(self):
        for task in self.tasks:
            task.clean()
    
    def kill(self, test=False):
        if self.is_array:
            cmd_args = ["bkill", "-g", "/" + self.job_group, "0"]
            if test:
                print(wrap_cmd_args(cmd_args))
            else:
                subprocess.call(cmd_args)
        else:
            for task in self.tasks:
                task.kill(test)
    
    def submit_array(self, test=False, skip_succeeded=False):
        R = "select[mem>{0}] rusage[mem={0}] span[hosts=1]".format(self.mem)
        outfile = '{}/{}_%I.out'.format(self.outdir, self.job_group)
        arrayScriptFileName = "{}/{}_array.sh".format(self.outdir, self.job_group)

        arrayScriptFile = open(arrayScriptFileName, "w")
        arrayScriptFile.write("#!/bin/bash\n\n")
        arrayScriptFile.write("COMMANDS=(dummy")
        for task in self.tasks:
            arrayScriptFile.write(' "{}"'.format(task.cmd))
        arrayScriptFile.write(")\n\n")
        arrayScriptFile.write('echo "${COMMANDS[$LSB_JOBINDEX]}"\n')
        arrayScriptFile.write('bash -c "${COMMANDS[$LSB_JOBINDEX]}"\n')
        
        index_spec = "1-{}".format(len(self.tasks))
        if skip_succeeded:
            indices = []
            for i, task in enumerate(self.tasks):
                if not task.check(quiet=True) == "SUCCESS":
                    indices.append(i + 1)
            index_spec = ",".join(map(str, indices))
        
        if index_spec == "":
            print("Nothing to be done for job_group {}".format(self.job_group))
        else:
            cmd_args = ['bsub', '-R', R, '-M', str(self.mem), '-n', str(self.threads), '-q', self.queue, '-G', self.farm_group, '-g', "/" + self.job_group, '-J', "{}[{}]".format(self.job_group, index_spec), '-oo', outfile, "bash {}".format(arrayScriptFileName)]

            if test:
                print(wrap_cmd_args(cmd_args))
            else:
                subprocess.call(cmd_args)
        
    def status_array(self, quiet=False):
        run = 0
        bjobs_out = subprocess.check_output(["bjobs", "-g", "/{}".format(self.job_group)], stderr=subprocess.STDOUT)
        lines = bjobs_out.decode().strip().split("\n")
        for line in lines:
            fields = line.split()
            if len(fields) > 2:
                if fields[2] == "RUN":
                    run += 1
                if not quiet:
                    print(line)
        print("{} of {} running: {}".format(run, len(self.tasks), self.job_group))


def cmd_ls(args):
    for tg in taskgroups:
        print("TaskGroup:", tg)
        for t in tg.tasks:
            print(t)

def cmd_print(args):
    for tg in taskgroups:
        tg.print_cmd()

def cmd_submit(args):
    for tg in taskgroups:
        tg.submit(test=args.test, skip_succeeded=args.skip_succeeded)

def cmd_check(args):
    for tg in taskgroups:
        tg.check(quiet=args.quiet)

def cmd_status(args):
    for tg in taskgroups:
        tg.status(quiet=args.quiet)

def cmd_kill(args):
    for tg in taskgroups:
        tg.kill(test=args.test)

def cmd_clean(args):
    for tg in taskgroups:
        tg.clean()

def cmd_cat(args):
    taskgroups[args.i].tasks[args.j].cat()


def jman_main():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers()
    
    parser_list = subparsers.add_parser("ls")
    parser_list.set_defaults(func=cmd_ls)
    
    parser_print = subparsers.add_parser("print")
    parser_print.set_defaults(func=cmd_print)

    parser_submit = subparsers.add_parser("submit")
    parser_submit.add_argument("-t", "--test", action="store_true", default=False)
    parser_submit.add_argument("-s", "--skip_succeeded", action="store_true", default=False)
    parser_submit.set_defaults(func=cmd_submit)
    
    parser_check = subparsers.add_parser("check")
    parser_check.add_argument("-q", "--quiet", action="store_true", default=False)
    parser_check.set_defaults(func=cmd_check)

    parser_status = subparsers.add_parser("status")
    parser_status.add_argument("-q", "--quiet", action="store_true", default=False)
    parser_status.set_defaults(func=cmd_status)

    parser_kill = subparsers.add_parser("kill")
    parser_kill.add_argument("-t", "--test", action="store_true", default=False)
    parser_kill.set_defaults(func=cmd_kill)

    parser_clean = subparsers.add_parser("clean")
    parser_clean.set_defaults(func=cmd_clean)

    parser_cat = subparsers.add_parser("cat")
    parser_cat.add_argument("i", type=int)
    parser_cat.add_argument("j", type=int)
    parser_cat.set_defaults(func=cmd_cat)
    
    args = parser.parse_args()
    args.func(args)
