#############################
#   Joris Kersten           #
#   j.kersten@astro.ru.nl   #
#   Tue 2024-05-14          #
#############################


# This program can be used to determine the percentage of flagged visibilities.
# The python-casacore package will be used to access MS tables.
# It is available in /idia/software/containers/kern8.sif for example.

# Currently, this is not a finished program.
# It currently works on the full FLAG column of a MeasurementSet, determining only a single percentage.
# Finding the flag percentage for a selection, or for iterative selections (each scan for example) are features which
# should be added in the future.


# Imports.
from pathlib import Path
import sys
import os
import datetime
from contextlib import suppress
import numpy as np
import numba as nb
from casacore.tables import table, tablecolumn


# Settings.
# container="/idia/software/containers/oxkat-0.42.sif"
container="/idia/projects/thunderkat/containers/OC/oxkat-0.5_vol1.sif"
# basedir="/scratch3/users/username/object/oxkatversion/sourcedir/"
basedir="../"
# scriptpath=basedir + "MYTOOLS/FlagPercentage.py"
ProcessMeasurementSets = True
MeasurementSetNumbers = [1,]   # These MeasurementSets will be processed if ProcessMeasurementSets is True.


# Print a welcome message.
print("")
print("FlagPercentage.py")
print("-----------------")
print("")
starttime = datetime.datetime.now(datetime.timezone.utc)
print("Current time:   {} (UTC)".format(starttime.replace(tzinfo=None).isoformat(sep=' ')))
print('')


@nb.jit
def count_nb(arr, value):
    result = 0
    for i, x in enumerate(arr):
        if x == value:
            result += 1
    return result

@nb.jit(parallel=True)
def count_nbp(arr, value):
    result = 0
    for i in nb.prange(arr.size):
        if arr[i] == value:
            result += 1
    return result

def count_np(arr, value):
    return np.count_nonzero(arr == value)

def printloopprogress(in_i, in_len, in_curtime, in_timedelta):
    cur_timetoprint = in_curtime.replace(tzinfo=None).isoformat(sep=' ')
    cur_timeremaining = in_timedelta * (in_len/(in_i+1) - 1.)
    cur_estimatedendtime = in_curtime + cur_timeremaining
    cur_estimatedendtimetoprint = cur_estimatedendtime.replace(tzinfo=None).isoformat(sep=' ', timespec='seconds')
    print("[{:6.2f}%    ( {:9} / {:9} )]    "
          "Time: {} (UTC)    Time in loop: {}    "
          "Estimated end time: {} (UTC)    Estimated time in loop remaining: {}"
          .format(100.*(in_i+1)/in_len, in_i+1, in_len,
                          cur_timetoprint, in_timedelta,
                          cur_estimatedendtimetoprint, str(cur_timeremaining).split('.')[0]))
# Function to get a flagging summary.
# One can run the flagging task 'flagdata´. It will give a summary of what it has done.
# It returns a dict, containing as keys the field names and 'name' and 'type'. It should probably have a key 'field',
# with the field names as subkeys.
# The way to get a percentage is to open the MS and manually go through a 'FLAG' column. There is no nice CASA task.
# The listobs task can give some information. It is of course as retarted as everything else realted to CASA.
# It will give the number of rows per field and the number of unflagged rows per field.
# It is not so easy to access this information.
# The second number can be non-integer.
def getflagsummary(in_vis):
    cur_flaginfo = flagdata(vis=in_vis, mode='summary', action='calculate', spwchan=False, fieldcnt=True)
    return cur_flaginfo

def printflagsummary(in_flaginfo):
    for cur_field in in_flaginfo.keys():
        cur_flagperc = 100.0 * flaginfo[cur_field]['flagged'] / flaginfo[cur_field]['total']
        print("Field: {}".format(cur_field))
        print("Flagged: {:3.2f}\n".format(cur_flagperc))

def getmaintableinfo(in_msfile):
    # Opening the main table of the specified MeasurementSet
    maintab = table(in_msfile)
    mainrows = maintab.nrows()
    print("Number of rows (unique timestamp and baseline): {}".format(mainrows))
    maincolnames = sorted(maintab.colnames())
    print("Columns present in the main table:")
    print(maincolnames)
    if "FLAG" in maincolnames:
        flagcol = tablecolumn(maintab, 'FLAG')   # flagcol is a 2-dim numpy array of shape (channels, correlations).
        total_points = np.shape(flagcol[0])[0]*np.shape(flagcol[0])[1]*len(flagcol)   # It is assumed that all rows have the same shape (channels and correlations).
        # total_points = 0   # This is used if a count is made.
        flagged_points = 0
        print("\nCounting flagged visibilities.")
        counting_starttime = datetime.datetime.now(datetime.timezone.utc)
        
        # flagged_points = count_nb(np.ravel(flagcol), False)
        
        for i, cur_flagrow in enumerate(flagcol):
            if i%10000 == 0:
                counting_time = datetime.datetime.now(datetime.timezone.utc)
                printloopprogress(i, len(flagcol), counting_time, counting_time-counting_starttime)

            # unique, counts = np.unique(cur_flagrow, return_counts=True)
            # cur_flagrow_countsdict = dict(zip(unique, counts))
            # if False in cur_flagrow_countsdict.keys():
            #     flagged_points += cur_flagrow_countsdict[False]

            # cur_flagrow_flattened = list(cur_flagrow.flatten())
            # flagged_points += cur_flagrow_flattened.count(True)
            # total_points += len(cur_flagrow_flattened)

            flagged_points += count_nb(np.ravel(cur_flagrow, order='K'), False)
        
        counting_endtime = datetime.datetime.now(datetime.timezone.utc)
        current_endtimetoprint = counting_endtime.replace(tzinfo=None).isoformat(sep=' ')
        print("Counting finished. Time: {} (UTC)    Time in loop: {}\n"
              .format(current_endtimetoprint, counting_endtime-counting_starttime))
        print("Result: {:6.2f}% of visibilities flagged. ( flagged points / total points: {} / {} )\n"
              .format(100.*flagged_points/total_points, flagged_points, total_points))
    maintab.close()
    print("Table 'maintab' closed.\n")



# Check if basedir is a directory.
BasedirPath = Path(basedir).resolve()
print("BasedirPath: {}\n".format(BasedirPath))
if not BasedirPath.exists():
    with suppress(OSError): os.fsync(sys.stdout.fileno())
    raise NotADirectoryError("Basedir {} does not exist.".format(BasedirPath))
if not BasedirPath.is_dir():
    with suppress(OSError): os.fsync(sys.stdout.fileno())
    raise NotADirectoryError("Basedir {} is not a directory.".format(BasedirPath))


# Get all MeasurementSets from the base directory.
MeasurementSets = sorted(BasedirPath.glob('*.ms/'))
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


# ContainerPath = Path(container).resolve()
# if not ContainerPath.exists():
#     with suppress(OSError): os.fsync(sys.stdout.fileno())
#     raise FileNotFoundError("Container file {} does not exist.".format(ContainerPath))
# if not ContainerPath.is_file():
#     with suppress(OSError): os.fsync(sys.stdout.fileno())
#     raise FileNotFoundError("Container file {} is exists but is not a file.".format(ContainerPath))


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
    print("\n----\n")


# Process the MeasurementSets.
for m in SelectedMeasurementSets:
    print("Processing {}\n".format(m))
    # cur_fs = getflagsummary(str(m))
    # print(cur_fs.keys())
    # printflagsummary(cur_fs)
    # listobs(vis=str(m),
    #         selectdata=True,
    #         spw='',
    #         field='',
    #         antenna='',
    #         uvrange='',
    #         timerange='',
    #         correlation='',
    #         scan='',
    #         intent='',
    #         feed='',
    #         array='',
    #         observation='',
    #         verbose=True,
    #         listfile=str(m.name)+'.log',
    #         listunfl=True,
    #         # cachesize=50,
    #         overwrite=True)
    # listobs(vis=str(m),
    #         selectdata=False,
    #         verbose=True,
    #         listfile=str(m.name)+'.log',
    #         listunfl=True,
    #         # cachesize=50,
    #         overwrite=True)
    # print("----\n")
    getmaintableinfo(str(m))
    print("----\n")


# Print the time at the end of the program and the program duration.
endtime = datetime.datetime.now(datetime.timezone.utc)
print("Current time:   {} (UTC)".format(endtime.replace(tzinfo=None).isoformat(sep=' ')))
print("Program duration: {}".format(endtime-starttime))
print('\n')
