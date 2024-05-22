#############################
#   Joris Kersten           #
#   j.kersten@astro.ru.nl   #
#   Tue 2024-05-14          #
#############################


# This program can be used to determine the percentage of flagged visibilities.
# The python-casacore package will be used to access MS tables.
# It is available in /idia/software/containers/kern8.sif for example.

# The CASA listobs task can give similar information.
# It will give the number of rows per field and the number of unflagged rows per field.
# The second number can be non-integer.

# Currently, this is not a finished program.
# It currently works on the full FLAG column of a MeasurementSet, determining only a single percentage.
# Finding the flag percentage for a selection, or for iterative selections (each scan for example) are features which
# should be added in the future.
# Also, in the future the program should be able to work with FLAGVERSION tables.


# Imports.
import logging
from pathlib import Path
import sys
import os
import datetime
from contextlib import suppress
import numpy as np
import numba as nb
from casacore.tables import table, tablecolumn

from MYHELPERSCRIPTS.loggingfunctions import logging_initialization, logandprint


# Initialize logging and print a welcome message and the time.
logging_initialization(Path(__file__).name)
logandprint("")
logandprint("FlagPercentage.py")
logandprint("-----------------")
logandprint("")
starttime = datetime.datetime.now(datetime.timezone.utc)
logandprint("Current time:   {} UTC".format(starttime.replace(tzinfo=None).isoformat(sep=' ')))
logandprint('')


# Settings.
# basedir="/scratch3/users/username/object/oxkatversion/sourcedir/"
basedir="../"
ProcessMeasurementSets = True
MeasurementSetNumbers = [1,]   # These MeasurementSets will be processed if ProcessMeasurementSets is True.


# A numba function to count occurrences of a certain value. It should be fast for larger numpy arrays.
# To count in a multidimensional array one can use np.ravel(arr) as input.
@nb.jit
def count_nb(in_arr, in_value):
    result = 0
    for x in in_arr:
        if x == in_value:
            result += 1
    return result

# # A parallel computing enabled numba function to count occurrences of a certain value.
# # It should be fast for larger numpy arrays.
# # To count in a multidimensional array one can use np.ravel(arr) as input.
# @nb.jit(parallel=True)
# def count_nbp(in_arr, in_value):
#     result = 0
#     for i in nb.prange(in_arr.size):
#         if in_arr[i] == in_value:
#             result += 1
#     return result
# 
# # A numpy function to count occurrences of a certain value.
# # It should be reasonably fast for larger numpy arrays.
# # To count in a multidimensional array one can use np.ravel(arr) as input.
# def count_np(in_arr, in_value):
#     return np.count_nonzero(in_arr == in_value)


# This function prints the progress of going through a loop.
# It requires the current iteration, the total number of iterations,
# the current time (as datetime with UTC timezone) and the time since the start of the loop (as timedelta).
def printloopprogress(in_i, in_len, in_curtime, in_timedelta):
    cur_timetoprint = in_curtime.replace(tzinfo=None).isoformat(sep=' ')
    cur_timeremaining = in_timedelta * (in_len/(in_i+1) - 1.)
    cur_estimatedendtime = in_curtime + cur_timeremaining
    cur_estimatedendtimetoprint = cur_estimatedendtime.replace(tzinfo=None).isoformat(sep=' ', timespec='seconds')
    cur_string = ("[{:6.2f}%  ( {:9} / {:9} )]  "
                  "Time: {} UTC  Time in loop: {}  "
                  "Estimated end time: {} UTC  Estimated loop time remaining: {}"
                  .format(100.*(in_i+1)/in_len, in_i+1, in_len,
                          cur_timetoprint, in_timedelta,
                          cur_estimatedendtimetoprint, str(cur_timeremaining).split('.')[0]))
    logandprint(cur_string)


# Function to get a flagging summary.
# One can run the flagging task 'flagdata´. It will give a summary of what it has done (either applied or as dry run).
# It returns a dict, containing as keys the field names and 'name' and 'type'. It should probably have a key 'field',
# with the field names as subkeys.
def flagandgetflagsummary(in_vis):
    cur_flaginfo = flagdata(vis=in_vis, mode='summary', action='calculate', spwchan=False, fieldcnt=True)
    return cur_flaginfo


# Print information from the summary of a CASA flagdata run. 
def printflagsummary(in_flaginfo):
    for cur_field in in_flaginfo.keys():
        cur_flagperc = 100.0 * flaginfo[cur_field]['flagged'] / flaginfo[cur_field]['total']
        logandprint("Field: {}".format(cur_field))
        logandprint("Flagged: {:3.2f}\n".format(cur_flagperc))


# This function is meant to quickly get information on a MeasurementSet MAIN TABLE.
# However, it now also scans the bools on the FLAG column.
def getmaintableinfo(in_msfile):
    # Opening the main table of the specified MeasurementSet
    maintab = table(in_msfile)
    mainrows = maintab.nrows()
    logandprint("Number of rows (unique timestamp and baseline): {}".format(mainrows))
    maincolnames = sorted(maintab.colnames())
    logandprint("Columns present in the main table:")
    logandprint(maincolnames)
    if "FLAG" in maincolnames:
        flagcol = tablecolumn(maintab, 'FLAG')   # flagcol is a 2-dim numpy array of shape (channels, correlations).
        total_points = np.shape(flagcol[0])[0]*np.shape(flagcol[0])[1]*len(flagcol)   # It is assumed that all rows have the same shape (channels and correlations).
        # total_points = 0   # This is used if a count is made.
        flagged_points = 0
        logandprint("\nCounting flagged visibilities.")
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

            flagged_points += count_nb(np.ravel(cur_flagrow, order='K'), True)
        
        counting_endtime = datetime.datetime.now(datetime.timezone.utc)
        current_endtimetoprint = counting_endtime.replace(tzinfo=None).isoformat(sep=' ')
        logandprint("Counting finished. Time: {} (UTC)    Time in loop: {}\n"
              .format(current_endtimetoprint, counting_endtime-counting_starttime))
        inner_result_string = ("Result: {:6.2f}% of visibilities flagged. ( flagged points / total points: {} / {} )\n"
                               .format(100.*flagged_points/total_points, flagged_points, total_points))
        logandprint(inner_result_string)
    maintab.close()
    logandprint("Table 'maintab' closed.\n")
    return inner_result_string


# Check if basedir is a directory.
BasedirPath = Path(basedir).resolve()
logandprint("BasedirPath: {}\n".format(BasedirPath))
if not BasedirPath.exists():
    with suppress(OSError): os.fsync(sys.stdout.fileno())
    cur_error = NotADirectoryError("Basedir {} does not exist.".format(BasedirPath))
    logging.error(cur_error)
    raise cur_error
if not BasedirPath.is_dir():
    with suppress(OSError): os.fsync(sys.stdout.fileno())
    cur_error = NotADirectoryError("Basedir {} is not a directory.".format(BasedirPath))
    logging.error(cur_error)
    raise cur_error


# Get all MeasurementSets from the base directory.
MeasurementSets = sorted(BasedirPath.glob('*.ms/'))
if len(MeasurementSets) >= 1:
    logandprint("MeasurementSets found:")
    for i, m in enumerate(MeasurementSets):
        logandprint("[{:2}]: {}".format(i+1,m))
    logandprint("")
else:
    with suppress(OSError): os.fsync(sys.stdout.fileno())
    cur_error = FileNotFoundError("No MeasurementSet found.")
    logging.error(cur_error)
    raise cur_error


# Quit if processing was not requested.
if not ProcessMeasurementSets:
    logandprint("Further processing was not requested. The program will end now.")
    sys.exit(0)   # This is not an error, so we simply quit the program here.
else:
    logandprint("Processing MeasurementSets.\n")


# ContainerPath = Path(container).resolve()
# if not ContainerPath.exists():
#     with suppress(OSError): os.fsync(sys.stdout.fileno())
#     cur_error = FileNotFoundError("Container file {} does not exist.".format(ContainerPath))
#     logging.error(cur_error)
#     raise cur_error
# if not ContainerPath.is_file():
#     with suppress(OSError): os.fsync(sys.stdout.fileno())
#     cur_error = FileNotFoundError("Container file {} is exists but is not a file.".format(ContainerPath))
#     logging.error(cur_error)
#     raise cur_error


# Select the MeasurementSets which should be processed.
SelectedMeasurementSets = []
for i in MeasurementSetNumbers:
    if i>0 and i<=len(MeasurementSets):
        SelectedMeasurementSets.append(MeasurementSets[i-1])
if len(SelectedMeasurementSets) <=0:
    with suppress(OSError): os.fsync(sys.stdout.fileno())
    cur_error = ValueError("No MeasurementSet is selected. MeasurementSetNumbers does not contain a valid index.")
    logging.error(cur_error)
    raise cur_error
else:
    logandprint("Selected Measurementsets:")
    for i,m in enumerate(SelectedMeasurementSets):
        logandprint("({:2}): {}".format(i+1,m))
    logandprint("\n----\n")


# Process the MeasurementSets.
for m in SelectedMeasurementSets:
    logandprint("Processing {}\n".format(m))
    
    # cur_fs = flagandgetflagsummary(str(m))
    # logandprint(cur_fs.keys())
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
    # logandprint("----\n")
    
    result_string = getmaintableinfo(str(m))
    logandprint("----\n")


# Print the time at the end of the program (since this is the last part of it) and the program duration.
endtime = datetime.datetime.now(datetime.timezone.utc)
logandprint("Current time:   {} UTC".format(endtime.replace(tzinfo=None).isoformat(sep=' ')))
logandprint("Program duration: {}".format(endtime-starttime))
logandprint('\n')
