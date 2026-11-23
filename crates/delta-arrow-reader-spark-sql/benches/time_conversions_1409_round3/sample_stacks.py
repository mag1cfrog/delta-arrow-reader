"""Sample the stacks of a local SQL benchmark launched by LLDB."""

import json
from pathlib import Path
import shlex
import time

import lldb


def wait_for_state(process, states, deadline):
    while (state := process.GetState()) not in states:
        if time.monotonic() >= deadline:
            raise TimeoutError("benchmark process did not reach the expected state")
        time.sleep(0.002)
    return state


def frames(thread):
    return [
        {
            "function": frame.GetFunctionName(),
            "source": frame.GetLineEntry().GetFileSpec().GetFilename(),
            "line": frame.GetLineEntry().GetLine(),
        }
        for frame in list(thread)[:24]
    ]


def sample_stacks(debugger, command, result, _):
    output, *arguments = shlex.split(command)
    directory = Path(output)
    try:
        directory.mkdir()
    except OSError as error:
        result.SetError(str(error))
        return
    launch = lldb.SBLaunchInfo(arguments)
    launch.SetLaunchFlags(launch.GetLaunchFlags() & ~lldb.eLaunchFlagDisableASLR)
    for fd, name in ((1, "stdout.jsonl"), (2, "stderr.log")):
        launch.AddOpenFileAction(fd, str(directory / name), False, True)
    debugger.SetAsync(True)
    error = lldb.SBError()
    process = debugger.GetSelectedTarget().Launch(launch, error)
    if error.Fail():
        result.SetError(error.GetCString())
        return

    deadline = time.monotonic() + 390
    exited = lldb.eStateExited
    stopped = lldb.eStateStopped
    running = lldb.eStateRunning
    try:
        with (directory / "stacks.jsonl").open("x") as output_file:
            for index in range(128):
                if wait_for_state(process, (running, exited), deadline) == exited:
                    break
                time.sleep((11 + index % 7) / 1000)
                error = process.Stop()
                if process.GetState() == exited:
                    break
                if error.Fail():
                    raise RuntimeError(error.GetCString())
                if wait_for_state(process, (stopped, exited), deadline) == exited:
                    break
                sample = {"index": index, "threads": [frames(thread) for thread in process]}
                output_file.write(json.dumps(sample) + "\n")
                output_file.flush()
                error = process.Continue()
                if error.Fail():
                    raise RuntimeError(error.GetCString())
        wait_for_state(process, (exited,), deadline)
        if process.GetExitStatus() != 0:
            raise RuntimeError(f"benchmark exited with {process.GetExitStatus()}")
    except (OSError, RuntimeError) as error:
        if process.GetState() != exited:
            process.Kill()
        result.SetError(str(error))


def __lldb_init_module(debugger, _):
    debugger.HandleCommand("command script add -f sample_stacks.sample_stacks sample-stacks")
