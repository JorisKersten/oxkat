#############################
#   Joris Kersten           #
#   j.kersten@astro.ru.nl   #
#   Fri 2024-06-07          #
#############################


# This program can be used to determine the percentage of flagged visibilities.
# The python-casacore package will be used to access MS tables.
# It is available in /idia/software/containers/kern8.sif for example.
# Alternatively. the casatools package can be used to access MS tables. It can be pip-installed in a python 3.10 env.
# It may be available in /idia/software/containers/kern8.sif .

# The CASA listobs task can give similar information.
# It will give the number of rows per field and the number of unflagged rows per field.
# The second number can be non-integer.

# The CASA flagdata task can also give similar information. It can be used with the 'calculate' action.
# The 'summary' mode gives currently active flags. One can also 'calculate' (not apply) new flags with other modes. 
# Not all produced numbers are reliable. But the final flag percentage seems to be.

# Currently, this is not a finished program.
# It currently works on the full FLAG column of a MeasurementSet, determining a single percentage.
# The program not only counts the total flag percentage, but also per antenna (not considering autocorrelations)
# and per scan.
# Finding the flag percentage for a finer selection is a feature which should be added in the future.

# Also, in the future the program should be able to work with FLAGVERSION tables.
# These usually only have the FLAG and FLAG_ROW column, so they either need a corresponding MS main table or
# one has to restrict the data selection. Without MS one can probably not select per scan data or per antenna data.
# This functionality could be in another script.


# Imports.
import logging
from pathlib import Path
import sys
import os
import datetime
from contextlib import suppress
import numpy as np
import pandas as pd
# from casatools import table as tb   # This is unnecessary. It is not faster than python-casacore.
from casacore.tables import table as tb, taql

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
# basedir="/home/joris/Datadir/ObservationData/CasaTutorial3C391/"
# basedir="/home/joris/Datadir/PhDProject/MyObjects/V603Aql/V603Aql20230602/"
ProcessMeasurementSets = False
MeasurementSetNumbers = [1,]   # These MeasurementSets will be processed if ProcessMeasurementSets is True.


# Write the TaQL query, run it, get the result into a dataframe (df). Then return the df.
def count_flags_per_antenna(in_maintab, in_printmessages=False):
    inner_query = """
    with [select ANTENNA1, ANTENNA2, gntrue(FLAG) as NFLAG, gnfalse(FLAG) as NCLEAR
          from {}
          where ANTENNA1!=ANTENNA2
          groupby ANTENNA1,ANTENNA2] as t1
    select ANTENNA, gsum(NFLAG) as flagged, gsum(NCLEAR) as clear
    from [[select NFLAG,NCLEAR,ANTENNA1 as ANTENNA from t1],  
          [select NFLAG,NCLEAR,ANTENNA2 as ANTENNA from t1]]
    groupby ANTENNA orderby ANTENNA
    """.format(in_maintab.name())

    if in_printmessages:
        print("inner_query: {}".format(inner_query))
    
    inner_result = taql(inner_query)
    
    if in_printmessages:
        print("Result:")
        print("Type: {}".format(type(inner_result)))
        print(inner_result)
        print('\n')
        for i in range(len(inner_result)):
            print(inner_result[i])
        print('\n')
    
    mytable = []
    for i in range(inner_result.nrows()):
        mytable.append(inner_result[:][i])
    df = pd.DataFrame(mytable)
    df['total'] = df['flagged'] + df['clear']
    df['percentage'] = 100. * (df['flagged'] / df['total'])
    
    if in_printmessages:
        with pd.option_context('display.max_rows', None,
                               'display.max_columns', None,
                               'display.precision', 2,
                               ):
            print(df)
        print('\n')
    
    return df


# Write the TaQL query, run it, get the result into a dataframe (df). Then return the df.
def count_flags_per_scan(in_maintab, in_printmessages=False):
    inner_query = """
    select SCAN_NUMBER, gntrue(FLAG) as flagged, gnfalse(FLAG) as clear
    from {}
    where ANTENNA1!=ANTENNA2
    groupby SCAN_NUMBER orderby SCAN_NUMBER
    """.format(in_maintab.name())
    
    if in_printmessages:
        print("inner_query: {}".format(inner_query))
    
    inner_result = taql(inner_query)
    
    if in_printmessages:
        print("Result:")
        print("Type: {}".format(type(inner_result)))
        print(inner_result)
        print('\n')
        for i in range(len(inner_result)):
            print(inner_result[i])
        print('\n')
    
    mytable = []
    for i in range(inner_result.nrows()):
        mytable.append(inner_result[:][i])
    df = pd.DataFrame(mytable)
    df['total'] = df['flagged'] + df['clear']
    df['percentage'] = 100. * (df['flagged'] / df['total'])
    
    if in_printmessages:
        with pd.option_context('display.max_rows', None,
                               'display.max_columns', None,
                               'display.precision', 2,
                               ):
            print(df)
        print('\n')

    return df


# A count is done with the calc method. It internally translates to TaQL.
def count_flags_inner(in_maintab):
    inner_calcexpr = 'sum([select from {} giving [nTrue(FLAG)]])'.format(in_maintab.name())
    inner_result = in_maintab.calc(expr=inner_calcexpr)
    return inner_result[0]


def count_flags_total(in_maintab):
    counting_starttime = datetime.datetime.now(datetime.timezone.utc)
    inner_flagged_points = count_flags_inner(in_maintab)
    counting_endtime = datetime.datetime.now(datetime.timezone.utc)
    current_endtimetoprint = counting_endtime.replace(tzinfo=None).isoformat(sep=' ')
    logandprint("Counting finished. Time: {} (UTC)    Time spent counting: {}\n"
                .format(current_endtimetoprint, counting_endtime - counting_starttime))
    return inner_flagged_points


# This function is meant to quickly get information on a MeasurementSet MAIN TABLE.
# However, it now also scans the bools on the FLAG column.
def getmaintableinfo(in_msfile):
    # Opening the main table of the specified MeasurementSet
    maintab = tb(in_msfile)
    mainrows = maintab.nrows()
    logandprint("Number of rows (unique timestamp and baseline): {}".format(mainrows))
    maincolnames = sorted(maintab.colnames())
    logandprint("Columns present in the main table:")
    logandprint(maincolnames)
    logandprint('\n----\n')
    if "FLAG" in maincolnames:
        logandprint("Flagged visibilities per antenna. Self-correlations are not considered.")
        perantennaresult = count_flags_per_antenna(maintab)
        with pd.option_context('display.max_rows', None,
                               'display.max_columns', None,
                               'display.precision', 2,
                               ):
            logandprint(perantennaresult)
        logandprint('\n----\n')
        logandprint("Flagged visibilities per scan. Self-correlations are not considered.")
        perscanresult = count_flags_per_scan(maintab)
        with pd.option_context('display.max_rows', None,
                               'display.max_columns', None,
                               'display.precision', 2,
                               ):
            logandprint(perscanresult)
        logandprint('\n')
        perscantotalflagged = perscanresult['flagged'].sum()
        perscantotalclear = perscanresult['clear'].sum()
        logandprint("Total flags, summed from the per scan result. Self-correlations are excluded.")
        logandprint("Flagged: {}   Total: {}    Percentage: {:.4}%\n"
                    .format(perscantotalflagged, perscantotalflagged+perscantotalclear,
                            100.*perscantotalflagged/(perscantotalflagged+perscantotalclear)))
        logandprint('----\n')
        flagcol = maintab.getcol('FLAG')   # flagcol is a 2-dim numpy array of shape (channels, correlations).
        flagcol_len = len(flagcol)
        total_points = np.shape(flagcol[0])[0]*np.shape(flagcol[0])[1]*flagcol_len   # It is assumed that all rows have the same shape (channels and correlations).
        logandprint("Counting flagged visibilities, including self-correlations.")
        flagged_points = count_flags_total(maintab)
        inner_result_string = ("Result: {:6.2f}% of visibilities flagged. ( flagged points / total points: {} / {} )\n"
                               .format(100. * flagged_points / total_points, flagged_points, total_points))
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


# Select the MeasurementSets which should be processed.
SelectedMeasurementSets = []
for i in MeasurementSetNumbers:
    if (i > 0) and (i <= len(MeasurementSets)):
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
    result_string = getmaintableinfo(str(m))
    logandprint("----\n")


# Print the time at the end of the program (since this is the last part of it) and the program duration.
endtime = datetime.datetime.now(datetime.timezone.utc)
logandprint("Current time:   {} UTC".format(endtime.replace(tzinfo=None).isoformat(sep=' ')))
logandprint("Program duration: {}".format(endtime-starttime))
logandprint('\n')
