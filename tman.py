import json

def makeTask(name, command, inputFiles=[], outputFiles=[], mem=100, nrThreads=1, submissionQueue="normal", submissionGroup=""):
    task = {"name":name, "inputFiles":inputFiles, "outputFiles":outputFiles, "command":command, "mem":mem,
            "nrThreads":nrThreads, "submissionQueue":submissionQueue, "submissionGroup":submissionGroup}
    print(json.dumps(task))
