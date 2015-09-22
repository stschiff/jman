import json

def makeTask(name, command, inputTasks=[], inputFiles=[], outputFiles=[], mem=100, nrThreads=1, hours=12):
    task = {"name":name, "inputTasks":inputTasks, "inputFiles":inputFiles, "outputFiles":outputFiles, "command":command, 
            "mem":mem, "nrThreads":nrThreads, "hours":hours}
    print(json.dumps(task))
