#############################
#   Joris Kersten           #
#   j.kersten@astro.ru.nl   #
#   Fri 2023-06-30          #
#############################


# This program can be used to list MeasurementSets and then
# run three MSINFO scripts on a chosen subset of the found MeasurementSets.


# Imports.
from pathlib import Path
import sys
import os
from contextlib import suppress
from subprocess import call
from datetime import datetime, timezone


# Settings.
container="/idia/software/containers/oxkat-0.42.sif"
basedir="/scratch3/users/name/dir/"
scriptdir=basedir
ProcessMeasurementSets = True
MeasurementSetNumbers = [1,]   # These MeasurementSets will be processed if ProcessMeasurementSets is True.


# Print a welcome message.
print("MSINFO script runner")
print("--------------------")
print("")


# Check if basedir is a directory.
BasedirPath = Path(basedir)
print("BasedirPath: {}\n".format(BasedirPath))
if not BasedirPath.exists():
    with suppress(OSError): os.fsync(sys.stdout.fileno())
    raise NotADirectoryError("Basedir {} does not exist.".format(BasedirPath))
if not BasedirPath.is_dir():
    with suppress(OSError): os.fsync(sys.stdout.fileno())
    raise NotADirectoryError("Basedir {} is not a directory.".format(BasedirPath))


# Get all MeasurementSets from the base directory.
MeasurementSets = list(BasedirPath.glob('*.ms/'))
if len(MeasurementSets) >= 1:
    print("MeasurementSets found:")
    for i, m in enumerate(MeasurementSets):
        print("[{:2}]: {}".format(i+1,m))
    print("")
else:
    with suppress(OSError): os.fsync(sys.stdout.fileno())
    raise FileNotFoundError("No MeasurementSet found.")


# Quit if processing was not requested.
if not ProcessMeasurementSets:
    print("Further processing was not requested. The program will end now.")
    sys.exit(0)
else:
    print("Processing MeasurementSets.\n")


ContainerPath = Path(container)
if not ContainerPath.exists():
    with suppress(OSError): os.fsync(sys.stdout.fileno())
    raise FileNotFoundError("Container file {} does not exist.".format(ContainerPath))
if not ContainerPath.is_file():
    with suppress(OSError): os.fsync(sys.stdout.fileno())
    raise NotAFileError("Container file {} is not a file.".format(ContainerPath))


# Check if scriptdir is a directory and locate and check the script files.
ScriptdirPath = Path(scriptdir)
print("ScriptdirPath: {}\n".format(ScriptdirPath))
if not ScriptdirPath.exists():
    with suppress(OSError): os.fsync(sys.stdout.fileno())
    raise NotADirectoryError("Scriptdir {} does not exist.".format(ScriptdirPath))
if not ScriptdirPath.is_dir():
    with suppress(OSError): os.fsync(sys.stdout.fileno())
    raise NotADirectoryError("Scriptdir {} is not a directory.".format(ScriptdirPath))
MSInfoScriptPath = ScriptdirPath / "ms_info.py"
if not MSInfoScriptPath.exists():
    with suppress(OSError): os.fsync(sys.stdout.fileno())
    raise FileNotFoundError("Script file {} does not exist.".format(MSInfoScriptPath))
if not MSInfoScriptPath.is_file():
    with suppress(OSError): os.fsync(sys.stdout.fileno())
    raise NotAFileError("Script file {} is not a file.".format(MSInfoScriptPath))
ScantimesScriptPath = ScriptdirPath / "scan_times.py"
if not ScantimesScriptPath.exists():
    with suppress(OSError): os.fsync(sys.stdout.fileno())
    raise FileNotFoundError("Script file {} does not exist.".format(ScantimesScriptPath))
if not ScantimesScriptPath.is_file():
    with suppress(OSError): os.fsync(sys.stdout.fileno())
    raise NotAFileError("Script file {} is not a file.".format(ScantimesScriptPath))
FindsunScriptPath = ScriptdirPath / "find_sun.py"
if not FindsunScriptPath.exists():
    with suppress(OSError): os.fsync(sys.stdout.fileno())
    raise FileNotFoundError("Script file {} does not exist.".format(FindsunScriptPath))
if not FindsunScriptPath.is_file():
    with suppress(OSError): os.fsync(sys.stdout.fileno())
    raise NotAFileError("Script file {} is not a file.".format(FindsunScriptPath))


# Select the MeasurementSets which should be processed.
SelectedMeasurementSets = []
for i in MeasurementSetNumbers:
    if i>0 and i<=len(MeasurementSets):
        SelectedMeasurementSets.append(MeasurementSets[i-1])
if len(SelectedMeasurementSets) <=0:
    with suppress(OSError): os.fsync(sys.stdout.fileno())
    raise ValueError("No MeasurementSet is selected. MeasurementSetNumbers does not contain a valid index.") 
else:
    print("Selected Measurementsets:")
    for i,m in enumerate(SelectedMeasurementSets):
        print("({:2}): {}".format(i+1,m))
    print("")


# Check if MSINFO exists, and if not, create it.
MSINFOPath = BasedirPath / "MSINFO"
if not MSINFOPath.exists():
    print("MSINFO subdirectory created.")
    MSINFOPath.mkdir()
if not MSINFOPath.is_dir():
    with suppress(OSError): os.fsync(sys.stdout.fileno())
    raise NotADirectoryError("MSINFOPath {} is not a directory.".format(MSINFOPath))


# Process the MeasurementSets.
for m in SelectedMeasurementSets:
    print("Processing {}".format(m))
    cur_time=datetime.now(timezone.utc).isoformat(timespec='seconds').replace('-','').replace(':','')[:-5]
    print(cur_time)
    cur_OutputPath = MSINFOPath / cur_time
    cur_OutputPath.mkdir()
    os.chdir(cur_OutputPath)
    MSLinkPath = cur_OutputPath / m.name
    MSLinkPath.symlink_to(m, target_is_directory=True)
    # Run python ms_info.py [symlink] in the subdirectory. Same for the two other scripts.
    try:
        print("")
        retcode = call("singularity exec {} python3 {} {}".format(ContainerPath, MSInfoScriptPath, MSLinkPath.name), cwd=cur_OutputPath, shell=True)
        if retcode < 0:
            print("Child was terminated by signal", -retcode, file=sys.stderr)
        else: print("Child returned", retcode, file=sys.stderr)
    except OSError as e:
        print("Execution failed:", e, file=sys.stderr)
    try:
        print("")
        retcode = call("singularity exec {} python3 {} {}".format(ContainerPath, ScantimesScriptPath, MSLinkPath.name), cwd=cur_OutputPath, shell=True)
        if retcode < 0:
            print("Child was terminated by signal", -retcode, file=sys.stderr)
        else: print("Child returned", retcode, file=sys.stderr)
    except OSError as e:
        print("Execution failed:", e, file=sys.stderr)
    try:
        print("")
        retcode = call("singularity exec {} python3 {} {}".format(ContainerPath, FindsunScriptPath, MSLinkPath.name), cwd=cur_OutputPath, shell=True)
        if retcode < 0:
            print("Child was terminated by signal", -retcode, file=sys.stderr)
        else: print("Child returned", retcode, file=sys.stderr)
    except OSError as e:
        print("Execution failed:", e, file=sys.stderr)
