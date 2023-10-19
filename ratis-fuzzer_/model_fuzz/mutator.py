import random
import traceback

class DefaultMutator:
    def __init__(self) -> None:
        pass

    def mutate(self, trace):
        new_trace = []
        for e in trace:
            new_trace.append(e)
        return new_trace

class RandomMutator():
    def __init__(self) -> None:
        pass

    def mutate(self, trace):
        return None
    
class SwapMutator:
    def __init__(self) -> None:
        pass

    def mutate(self, trace: list[dict], num_crashes: int, nodes: int) -> list[dict]:
        # TODO - min 10
        new_trace = []
        for _ in range(10):
            new_trace = []
            schedule_steps = []
            for e in trace:
                if e["type"] == "Schedule":
                    schedule_steps.append(e["step"])
            [first, second] = random.sample(schedule_steps, 2)
            first_value = {"type": "Schedule", "node": 1, "node2": 2, "step": 101, "max_messages": 5}
            second_value = {"type": "Schedule", "node": 1, "node2": 2, "step": 102, "max_messages": 5}
            for e in trace:
                if e['type'] == 'Schedule' and e["step"] == first:
                    first_value = {"type": "Schedule", "node": e["node"], "node2": e["node2"], "step": e["step"], "max_messages": e["max_messages"]}
                elif e['type'] == 'Schedule' and e["step"] == second:
                    second_value = {"type": "Schedule", "node": e["node"], "node2": e["node2"], "step": e["step"], "max_messages": e["max_messages"]}
            
            for e in trace:
                if e["type"] != "Schedule":
                    new_trace.append(e)
                if e['type'] == 'Schedule' and e["step"] == first:
                    new_trace.append(second_value)
                elif e['type'] == 'Schedule' and e["step"] == second:
                    new_trace.append(first_value)
                else:
                    new_trace.append(e)
            trace = new_trace
        
        return new_trace
    
class SwapCrashStepsMutator:
    def __init__(self) -> None:
        pass

    def mutate(self, trace: list[dict], num_crashes: int, nodes: int) -> list[dict]:
        new_trace = []

        if num_crashes > 1:
            crash_steps = set()
            for e in trace:
                if e["type"] == "Crash":
                    crash_steps.add(e["step"])
            
            [first, second] = random.sample(list(crash_steps), 2)
            for e in trace:
                if e["type"] != "Crash":
                    new_trace.append(e)
                
                if e["step"] == first:
                    new_trace.append({"type": "Crash", "node": e["node"], "step": second})
                elif e["step"] == second:
                    new_trace.append({"type": "Crash", "node": e["node"], "step": first})
                else:
                    new_trace.append(e)
        else:
            try:
                crash_event = None
                restart_event = None
                schedule_events = []
                for e in trace:
                    if e["type"] == "Crash":
                        crash_event = e
                    elif e["type"] == "Start":
                        restart_event = e
                    elif e["type"] == "Schedule":
                        schedule_events.append(e["step"])
                
                new_steps = random.sample(schedule_events, 2)
                new_steps = sorted(new_steps)

                first = None
                second = None
                for e in trace:
                    if e['type'] == 'Schedule' and e["step"] == new_steps[0]:
                        first = e
                    elif e['type'] == 'Schedule' and e["step"] == new_steps[1]:
                        second = e
                
                if first is None or second is None:
                    return trace
                else:
                    crash_step = crash_event["step"]
                    restart_step = restart_event["step"]

                    crash_event["step"] = first["step"]
                    restart_event["step"] = second["step"]
                    first["step"] = crash_step
                    second["step"] = restart_step
                
                for e in trace:
                    if e['type'] == 'Schedule' and e["step"] == crash_event["step"]:
                        new_trace.append(crash_event)
                    elif e['type'] == 'Schedule' and e["step"] == restart_event["step"]:
                        new_trace.append(restart_event)
                    elif e['type'] == 'Crash' and e["step"] == first["step"]:
                        new_trace.append(first)
                    elif e['type'] == 'Start' and e["step"] == second["step"]:
                        new_trace.append(second)
                    else:
                        new_trace.append(e)
            except Exception as ex:
                pass
            finally:
                return trace
        return new_trace
    
class SwapCrashNodesMutator:
    def __init__(self) -> None:
        pass

    def mutate(self, trace: list[dict], num_crashes: int, nodes: int) -> list[dict]:
        new_trace = []

        if num_crashes > 1:
            crash_steps = {}
            for e in trace:
                if e["type"] == "Crash":
                    crash_steps[e["step"]] = e["node"]
            
            [first, second] = random.sample(list(crash_steps.keys()), 2)
            for e in trace:
                if e["type"] != "Crash":
                    new_trace.append(e)
                
                if e["step"] == first:
                    new_trace.append({"type": "Crash", "node":crash_steps[second], "step": e["step"]})
                elif e["step"] == second:
                    new_trace.append({"type": "Crash", "node":crash_steps[first], "step": e["step"]})
                else:
                    new_trace.append(e)
        else:
            try:
                crash_event = None
                restart_event = None
                for e in trace:
                    try:
                        if e["type"] == "Crash":
                            crash_event = e
                        elif e["type"] == "Start":
                            restart_event = e
                    except:
                        pass
                if crash_event is None or restart_event is None:
                    return trace
                new_nodes = [node for node in range(1,nodes+1) if node != crash_event["node"]]
                n = random.choice(new_nodes)
                crash_event["node"] = n
                restart_event["node"] = n

                for e in trace:
                    if e['type'] == 'Crash' and e["step"] == crash_event["step"]:
                        new_trace.append(crash_event)
                    elif e['type'] == 'Restart' and e["step"] == restart_event["step"]:
                        new_trace.append(restart_event)
                    else:
                        new_trace.append(e)
            except Exception as ex:
               pass
            finally:
                return trace
        return new_trace


class CombinedMutator:
    def __init__(self, mutators) -> None:
        self.mutators = mutators
    
    def mutate(self, trace: list[dict], num_crashes: int, nodes: int) -> list[dict]:
        new_trace = []
        for e in trace:
            new_trace.append(e)
        
        for m in self.mutators:
            new_trace = m.mutate(new_trace, num_crashes, nodes)
        
        return new_trace