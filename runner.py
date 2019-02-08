#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import absolute_import, print_function

import os
import re
import time
import shutil
import argparse
import subprocess

DESCRIPTION = "Priroda runner"
TASKS = {"Hessian": "Hes", "Optimize": "Opt"}


def wait_timeout(process, timeout=0, interval=1):
    """ Wait for a process to finish, or raise exception after timeout """
    start = time.time()
    if timeout:
        end = start + timeout
    else:
        end = None
    while True:
        result = process.poll()
        if result is not None:
            return result
        if end and time.time() >= end:
            subprocess.call(["kill", "-SIGINT", str(process.pid)])
            raise RuntimeError("Process timed out")
        time.sleep(interval)


def start_process(process_args):
    """ Start sub process """
    process = subprocess.Popen(process_args, stdout=subprocess.PIPE)
    wait_timeout(process)
    result = process.communicate()[0]
    return result


def begin(file_path, mpi=1, steps=None, max_steps=50):
    print("Input file: {}".format(file_path))
    print("MPI: {}".format(mpi))

    work_dir, file_name = os.path.split(file_path)
    file_name = file_name.rsplit(".", 1)[0]

    res = re.findall("(.+)_([0-9]+)_", file_name)
    if res:
        file_name, step = res[0]
        step = int(step)
    else:
        step = 1

    if mpi > 1:
        start_args = ["./mpiexec", "-n", str(mpi), "./p"]
    else:
        start_args = ["./p"]

    template = []
    in_file_path = file_path
    task = ""
    prev_vec = vec = ""
    opt_steps = 4
    molecule = []
    is_molecule = False

    with open(file_path) as infile:
        # TODO: optimize read
        for line in infile.readlines():
            if not is_molecule:
                if "$molecule" in line:
                    is_molecule = True
                    molecule.append(line)
                    continue
                if "read=0" in line:
                    line = line.replace("read=0", "read=1")
                if "Mix=1" in line:
                    line = line.replace("Mix=1", "Mix=0")
                # FIXME: use re
                if "task=" in line:
                    task = line.split("task=", 1)[-1].strip()
                    line = line.replace(task, "{task}")
                if "save=" in line:
                    vec = line.split("save=", 1)[-1].strip()
                    line = line.replace("save={}".format(vec), "save={vec}")
                    prev_vec = vec
                    vec = os.path.split(vec)[-1].rsplit(".", 1)[0]
                if "steps=" in line:
                    opt_steps = int(line.split("steps=", 1)[-1].split(" ", 1)[0])
                    line = line.replace("steps={}".format(opt_steps), "steps={steps}")
                template.append(line)
            else:
                molecule.append(line)
                if "$end" in line:
                    break

    is_result = False

    while True:
        print("Task: {}, step: {}".format(task, step))
        task = task.capitalize()
        file_suf = TASKS.get(task)
        if file_suf is None:
            raise Exception("Wrong task")

        out_file_path = os.path.join(work_dir,
                                     "{}_{:02d}_{}.out".format(file_name,
                                                               step,
                                                               file_suf))

        print("In: {}".format(in_file_path))
        print("Out: {}".format(out_file_path))
        start_time = time.time()

        # Start Priroda
        start_process(start_args + [in_file_path, out_file_path])

        ex_time = time.time() - start_time

        # TODO: More information
        print("Execution time: {:d}:{:02d} min".format(int(ex_time / 60),
                                                       int(ex_time % 60)))

        if is_result:
            print("DONE!")
            break

        if step >= max_steps:
            print("Maximum count of steps reached.\nFINISHED!")
            break

        # Parse out file
        mol = []
        eng = []
        with open(out_file_path) as infile:
            for line in infile.readlines():
                if task == "Hessian":
                    if "eng>" in line or "G(max)" in line:
                        eng.append(line)
                elif task == "Optimize":
                    if "eng>$Energy" in line:
                        eng = []
                    elif "mol>$molecule" in line:
                        mol = []

                    if "MOL>$molecule" in line:
                        mol = []
                        is_result = True

                    if "eng>" in line or "G(max)" in line:
                        eng.append(line)
                    elif "mol>" in line or "MOL>" in line:
                        mol.append(line)

        if is_result:
            print("OPTIMIZATION CONVERGED!\nStart last Hessian step")

        if task == "Hessian":
            task = "Optimize"
            mol = molecule

        elif task == "Optimize":
            task = "Hessian"
            step += 1

        if not mol or not eng:
            raise Exception("Error in out file")

        mol = list(map(lambda s: s.replace("MOL>", ""), mol))
        mol = list(map(lambda s: s.replace("mol>", ""), mol))
        eng = list(map(lambda s: s.replace("eng>", ""), eng))

        content = template + mol + eng
        molecule = mol

        # Copy vec
        new_vec = os.path.join(work_dir,
                               "{}_{:02d}_{}.VEC".format(vec, step,
                                                         TASKS.get(task)))
        shutil.copyfile(prev_vec, new_vec)
        prev_vec = new_vec

        # Create in file
        in_file_path = os.path.join(work_dir,
                                    "{}_{:02d}_{}.in".format(file_name, step,
                                                             TASKS.get(task)))
        if steps and task == "Optimize":
            if opt_steps < steps[0]:
                opt_steps = steps[0]
            elif opt_steps + steps[1] > steps[2]:
                opt_steps = steps[2]
            else:
                opt_steps += steps[1]
            print("Optimization steps: {}".format(opt_steps))

        with open(in_file_path, 'w') as infile:
            infile.write(''.join(content).format(task=task,
                                                 vec=new_vec,
                                                 steps=opt_steps))


def parse_args():
    """ Argument parser """
    parser = argparse.ArgumentParser(description=DESCRIPTION)
    parser.add_argument("-n", "--numprocs", type=int, default=1,
                        help="Count processes")
    parser.add_argument("-s", "--steps", type=int, nargs=3,
                        help="Steps")
    parser.add_argument("-i", "--input", type=str, required=True,
                        help="Priroda input file")

    args = parser.parse_args()
    if args.numprocs < 1:
        raise AttributeError("Count of processes must be >= 1")
    if args.steps:
        if args.steps[0] > args.steps[2]:
            raise AttributeError("Start optimization steps count must be less than max steps count")
    return args


if __name__ == '__main__':
    args = parse_args()
    # print(args)
    begin(args.input, args.numprocs, args.steps)
