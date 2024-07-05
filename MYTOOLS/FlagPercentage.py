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
# The casatools package does not have taql(), so python-casacore is still needed.
# A workaround for using cvasatools would be to use table_instance.taql() , but then the rows
# cannot be iterated over or accessed in any sane way. To get the number of rows one can use table_instance.nrows() ,
# but table_instance.row() returns an undocumented object.

# The CASA listobs task can give similar information.
# It will give the number of rows per field and the number of unflagged rows per field.
# The second number can be non-integer.

# The CASA flagdata task can also give similar information. It can be used with the 'calculate' action.
# The 'summary' mode gives currently active flags. One can also 'calculate' (not apply) new flags with other modes.
# Not all produced numbers are reliable. But the final flag percentage seems to be.

# The program can currently work on the full FLAG column of a MeasurementSet, determining a single percentage.
# This is the percentage of visibilities flagged, including auto-correlations.
# The program can not only count the total flag percentage, but also per antenna (not considering auto-correlations)
# and per scan (not considering auto-correlations) and per frequency channel (not considering auto-correlations).
# Finding the flag percentage for a finer selection is a feature which could be added in the future.

# Also, in the future the program should be able to work with FLAGVERSION tables.
# These usually only have the FLAG and FLAG_ROW column, so they either need a corresponding MS main table or
# one has to restrict the data selection. Without MS one can probably not select per scan data or per antenna data.
# This functionality could be in another script.


# Plotting settings.
PlotPerFrequency = True
Plot_MyDPI = 120
Plot_MyWidth = 1536
Plot_MyHeight = 1024
PlotDir = '.'   # Used to save plots if requested.
SavePlots = True
ShowPlots = False


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
if PlotPerFrequency:
    import matplotlib.pyplot as plt

from MYHELPERSCRIPTS.loggingfunctions import logging_initialization, logandprint
from MYHELPERSCRIPTS.stokestypes import stokes_types


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
MSSortOrder = 'MODIFIEDTIME'   # Recognised options: 'NAME' and 'MODIFIEDTIME'. Any other string means unsorted.
ProcessMeasurementSets = True   # If False the program will select but not process the MeasurementSets.
ProcessAllMeasurementSets = True   # If True the MeasurementSetNumbers list (defined below) is ignored.
MeasurementSetNumbers = [1,]   # These MeasurementSets will be processed, in the given order.


# Write the TaQL query, run it, get the result into a dataframe (df). Then return the df.
def count_flags_per_antenna(in_maintab, in_printmessages=False):
    inner_query = """
        with [select ANTENNA1, ANTENNA2, gntrue(FLAG) as NFLAG, gnfalse(FLAG) as NCLEAR
              from {0}
              where ANTENNA1!=ANTENNA2
              groupby ANTENNA1,ANTENNA2] as t1
        select ANTENNA, (select NAME from {0}::ANTENNA)[ANTENNA] as NAME, gsum(NFLAG) as flagged, gsum(NCLEAR) as clear
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
    
    if len(inner_result) == 0:
        df = pd.DataFrame(columns=['ANTENNA', 'NAME', 'flagged', 'clear', 'total', 'percentage'])
    else:
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
        select SCAN_NUMBER, (select NAME from ::FIELD)[FIELD_ID] as FIELD,
               gntrue(FLAG) as flagged, gnfalse(FLAG) as clear
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
    
    if len(inner_result) == 0:
        df = pd.DataFrame(columns=['SCAN_NUMBER', 'FIELD', 'flagged', 'clear', 'total', 'percentage'])
    else:
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


# This should count per spw and then per channel in the spw.
# SPECTRAL_WINDOW_ID goes to the SPECTRAL_WINDOW table. It has NAME, CHAN_FREQ and CHAN_WIDTH.
# The rownumber() calls are not accepted in the first query. This is strange, since this use follows the TaQL manual.
# The third query is a workaround: row numbers are added to the table as an extra column.
def count_flags_per_channel(in_maintab, in_printmessages=False):
    # inner_query = """
    #     select t2.SPECTRAL_WINDOW_ID, t3.NAME as SPECTRAL_WINDOW_NAME, t3.CHAN_FREQ, t3.CHAN_WIDTH,
    #            gntrues(FLAG) as flagged, gnfalses(FLAG) as clear, gntrues(FLAG) + gnfalses(FLAG) as total
    #     from {} as t1
    #         join ::DATA_DESCRIPTION as t2 on t1.DATA_DESC_ID=t2.rownumber()
    #         join ::SPECTRAL_WINDOW as t3 on t2.SPECTRAL_WINDOW_ID=t3.rownumber()
    #     where ANTENNA1!=ANTENNA2
    #     groupby t2.SPECTRAL_WINDOW_ID orderby t2.SPECTRAL_WINDOW_ID
    #     """.format(in_maintab.name())
    
    # inner_query = """
    #     with [select (select SPECTRAL_WINDOW_ID from ::DATA_DESCRIPTION)[DATA_DESC_ID] as SPECTRAL_WINDOW_ID,
    #            gntrues(FLAG) as flagged, gnfalses(FLAG) as clear, gntrues(FLAG) + gnfalses(FLAG) as total
    #           from {}
    #           where ANTENNA1!=ANTENNA2] as t1
    #     select *
    #     from t1
    #     groupby SPECTRAL_WINDOW_ID orderby SPECTRAL_WINDOW_ID
    #     """.format(in_maintab.name())
    
    inner_query = """
        with [select rownumber() as rownr1, * from {0}::DATA_DESCRIPTION] as t1,
             [select rownumber() as rownr2, * from {0}::SPECTRAL_WINDOW] as t2
        select t1.SPECTRAL_WINDOW_ID, t2.NAME as SPECTRAL_WINDOW_NAME, t2.CHAN_FREQ, t2.CHAN_WIDTH,
               mscal.polcol('CORR_TYPE') as CORR_TYPE,
               gntrues(FLAG) as flagged, gnfalses(FLAG) as clear, gntrues(FLAG) + gnfalses(FLAG) as total
        from {0} as t3
            join t1 on t3.DATA_DESC_ID=t1.rownr1
            join t2 on t1.SPECTRAL_WINDOW_ID=t2.rownr2
        where ANTENNA1!=ANTENNA2
        groupby t1.SPECTRAL_WINDOW_ID orderby t1.SPECTRAL_WINDOW_ID
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

    if len(inner_result) == 0:
        df = pd.DataFrame(columns=['SPECTRAL_WINDOW_ID', 'SPECTRAL_WINDOW_NAME',
                                   'CHAN_FREQ', 'CHAN_WIDTH', 'flagged', 'clear', 'total'])
    else:
        mytable = []
        for i in range(inner_result.nrows()):
            mytable.append(inner_result[:][i])
        df = pd.DataFrame(mytable)

    if in_printmessages:
        with pd.option_context('display.max_rows', None,
                               'display.max_columns', None,
                               'display.precision', 2,
                               ):
            print(df)
        print('\n')

    inner_perfreqresult = {}
    if not len(df) == len(set(df['SPECTRAL_WINDOW_ID'])):
        raise ValueError("SPECTRAL_WINDOW_ID is not unique for each row.")
    for i in range(len(df)):
        cur_df = pd.DataFrame()
        cur_df['CHAN_FREQ'] = np.ravel(df['CHAN_FREQ'][i])
        cur_df['CHAN_WIDTH'] = np.ravel(df['CHAN_WIDTH'][i])
        for j in range(len(df['flagged'][i][0])):
            # cur_df['flagged_{:03}'.format(j)] = list(zip(*df['flagged'][i]))[j]
            # cur_df['clear_{:03}'.format(j)] = list(zip(*df['clear'][i]))[j]
            # cur_df['total_{:03}'.format(j)] = list(zip(*df['total'][i]))[j]
            cur_corrtype = stokes_types[df['CORR_TYPE'][i][j]]
            cur_df['flagged_'+cur_corrtype] = list(zip(*df['flagged'][i]))[j]
            cur_df['clear_'+cur_corrtype] = list(zip(*df['clear'][i]))[j]
            cur_df['total_'+cur_corrtype] = list(zip(*df['total'][i]))[j]
            cur_df['percentage_'+cur_corrtype] = 100.*cur_df['flagged_'+cur_corrtype]/cur_df['total_'+cur_corrtype]
        inner_perfreqresult[df['SPECTRAL_WINDOW_ID'][i]] = {
            'SPECTRAL_WINDOW_ID':df['SPECTRAL_WINDOW_ID'][i],
            'SPECTRAL_WINDOW_NAME':df['SPECTRAL_WINDOW_NAME'][i],
            'data':cur_df}
    
    return inner_perfreqresult


# A count is done with the calc method. It internally translates to TaQL.
def count_flags_total(in_maintab):
    inner_calcexpr = 'sum([select from {} giving [nTrue(FLAG)]])'.format(in_maintab.name())
    inner_result = in_maintab.calc(expr=inner_calcexpr)
    return inner_result[0]


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
        countingperantenna_starttime = datetime.datetime.now(datetime.timezone.utc)
        perantennaresult = count_flags_per_antenna(maintab)
        countingperantenna_endtime = datetime.datetime.now(datetime.timezone.utc)
        countingperantenna_endtimetoprint = countingperantenna_endtime.replace(tzinfo=None).isoformat(sep=' ')
        logandprint("Counting finished. Time: {} (UTC)    Time spent counting: {}\n"
                    .format(countingperantenna_endtimetoprint,
                            countingperantenna_endtime - countingperantenna_starttime))
        if perantennaresult.empty:
            logandprint("No visibilities found (not considering auto-correlations).\n")
        else:
            with pd.option_context('display.max_rows', None,
                                   'display.max_columns', None,
                                   'display.precision', 2,
                                  ):
                logandprint(perantennaresult)
        logandprint('\n----\n')
        
        logandprint("Flagged visibilities per scan. Self-correlations are not considered.")
        countingperscan_starttime = datetime.datetime.now(datetime.timezone.utc)
        perscanresult = count_flags_per_scan(maintab)
        countingperscan_endtime = datetime.datetime.now(datetime.timezone.utc)
        countingperscan_endtimetoprint = countingperscan_endtime.replace(tzinfo=None).isoformat(sep=' ')
        logandprint("Counting finished. Time: {} (UTC)    Time spent counting: {}\n"
                    .format(countingperscan_endtimetoprint,
                            countingperscan_endtime - countingperscan_starttime))
        if perscanresult.empty:
            logandprint("No visibilities found (not considering auto-correlations).\n")
        else:
            with pd.option_context('display.max_rows', None,
                                   'display.max_columns', None,
                                   'display.precision', 2,
                                  ):
                logandprint(perscanresult)
            logandprint('\n')
            perscantotalflagged = perscanresult['flagged'].sum()
            perscantotalclear = perscanresult['clear'].sum()
            logandprint("Total flags, summed from the per scan result. Self-correlations are excluded.")
            logandprint("Flagged: {:10}   Total: {:10}    Percentage: {:6.2f}%\n"
                        .format(perscantotalflagged, perscantotalflagged+perscantotalclear,
                                100.*perscantotalflagged/(perscantotalflagged+perscantotalclear)))
        logandprint('----\n')
        
        logandprint("Flagged visibilities per frequency. Self-correlations are not considered.")
        countingperfreq_starttime = datetime.datetime.now(datetime.timezone.utc)
        perfreqresult = count_flags_per_channel(maintab)
        countingperfreq_endtime = datetime.datetime.now(datetime.timezone.utc)
        countingperfreq_endtimetoprint = countingperscan_endtime.replace(tzinfo=None).isoformat(sep=' ')
        logandprint("Counting finished. Time: {} (UTC)    Time spent counting: {}\n"
                    .format(countingperfreq_endtimetoprint,
                            countingperfreq_endtime - countingperfreq_starttime))
        if len(perfreqresult) == 0:
            logandprint("No visibilities found (not considering auto-correlations).\n")
        else:
            for cur_wsid in perfreqresult:
                cur_pd_to_print = perfreqresult[cur_wsid].copy()
                logandprint("Spectral window: {}   -   {}".format(cur_pd_to_print['SPECTRAL_WINDOW_ID'],
                                                                  cur_pd_to_print['SPECTRAL_WINDOW_NAME']))
                with pd.option_context('display.max_rows', None,
                                       'display.max_columns', None,
                                       'display.width', 1000,
                                       'display.precision', 2
                                       ):
                    cur_data_to_print = cur_pd_to_print['data'].copy()
                    cur_data_to_print['CHAN_FREQ'] = cur_data_to_print['CHAN_FREQ'].map("{:,.6f}".format)
                    cur_data_to_print['CHAN_WIDTH'] = cur_data_to_print['CHAN_WIDTH'].map("{:,.6f}".format)
                    logandprint(cur_data_to_print)
                    logandprint('')

        logandprint('----\n')
        
        flagcol = maintab.getcol('FLAG')   # flagcol is a 2-dim numpy array of shape (channels, correlations).
        flagcol_len = len(flagcol)
        total_points = np.shape(flagcol[0])[0]*np.shape(flagcol[0])[1]*flagcol_len   # It is assumed that all rows have the same shape (channels and correlations).
        logandprint("Counting all flagged visibilities, including self-correlations.")
        counting_starttime = datetime.datetime.now(datetime.timezone.utc)
        flagged_points = count_flags_total(maintab)
        counting_endtime = datetime.datetime.now(datetime.timezone.utc)
        current_endtimetoprint = counting_endtime.replace(tzinfo=None).isoformat(sep=' ')
        logandprint("Counting finished. Time: {} (UTC)    Time spent counting: {}\n"
                    .format(current_endtimetoprint, counting_endtime - counting_starttime))
        inner_result_string = ("Result: {:6.2f}% of visibilities flagged. ( flagged points / total points: {} / {} )\n"
                               .format(100. * flagged_points / total_points, flagged_points, total_points))
        logandprint(inner_result_string)
        logandprint('----\n')
    
    maintab.close()
    logandprint("Table 'maintab' closed.\n")
    return (perantennaresult, perscanresult, perfreqresult, inner_result_string)


# This function makes a histogram of flagged and total points for all channels (spw combined) for
# one polarization correlation. Usually, but not always, flags are applied over all polarization correlations together.
# So, usually each polarization correlation choice would result in the same plot.
def plot_perfequency_histogram(in_perfreqresult):
    if len(perfreqresult) > 0:
        fig, ax = plt.subplots(figsize=(Plot_MyWidth/Plot_MyDPI, Plot_MyHeight/Plot_MyDPI), dpi=Plot_MyDPI, layout='constrained')
        flagged_corr = ''
        total_corr = ''
        for cur_wsid in in_perfreqresult:
            cur_freq = in_perfreqresult[cur_wsid]['data']['CHAN_FREQ']/1.e6
            cur_width = in_perfreqresult[cur_wsid]['data']['CHAN_WIDTH']/1.e6
            # print(in_perfreqresult[cur_wsid]['data'].columns)
            # The third column (number 2) is assumed to be the first column with 'flagged' data.
            cur_flagged = in_perfreqresult[cur_wsid]['data'].iloc[:, 2]
            if flagged_corr == '':
                flagged_corr = cur_flagged.name
            if not (flagged_corr == cur_flagged.name):
                print("Not every spw has the same first flagged column name: {} has {}\n"
                      .format(cur_wsid, cur_flagged.name))
            # The fifth column (number 4) is assumed to be the first column with 'total' data,
            # and assumed to be related to the third column.
            cur_total = in_perfreqresult[cur_wsid]['data'].iloc[:, 4]
            if total_corr == '':
                total_corr = cur_total.name
            if not (total_corr == cur_total.name):
                print("Not every spw has the same first total column name: {} has {}\n"
                      .format(cur_wsid, cur_total.name))
            ax.bar(x=cur_freq, height=cur_total, width=cur_width, color='blue', zorder=1.5,
                   edgecolor='black', linewidth=1.)
            ax.bar(x=cur_freq, height=cur_flagged, width=cur_width, color='red', zorder=2.5,
                   edgecolor='black', linewidth=1.)
        ax.set_xlabel("Frequency (MHz)")
        ax.set_ylabel("Visibilities count: {} (red) and {} (blue)".format(flagged_corr, total_corr))
        return fig, ax


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
if MSSortOrder == 'NAME':
    MeasurementSets = sorted(BasedirPath.glob('*.ms/'))
elif MSSortOrder == 'MODIFIEDTIME':
    MeasurementSets = sorted(BasedirPath.glob('*.ms/'), key=lambda x: x.stat().st_mtime)
else:
    MeasurementSets = list(BasedirPath.glob('*.ms/'))
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
if ProcessAllMeasurementSets:
    SelectedMeasurementSets = MeasurementSets
else:
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
    logandprint("Selected MeasurementSets:")
    for i,m in enumerate(SelectedMeasurementSets):
        logandprint("({:2}): {}".format(i+1,m))
    logandprint("\n----\n")


# Convert PlotDir to a resolved path and check if this path points to a directory. 
if PlotPerFrequency and SavePlots:
    plotdir_path = Path(PlotDir).resolve()
    if not plotdir_path.is_dir():
        raise NotADirectoryError("The PlotDir path (plotdir_path) does not point to a directory.")


# Process the MeasurementSets.
for m in SelectedMeasurementSets:
    logandprint("Processing {}\n".format(m))
    perantennaresult, perscanresult, perfreqresult, result_string = getmaintableinfo(str(m))
    if PlotPerFrequency:
        fig, ax = plot_perfequency_histogram(perfreqresult)
        if SavePlots:
            plotfile_name_png = "FlaggedAndTotalHistogram_{}.png".format(m.name)
            plotfile_path_png = plotdir_path / plotfile_name_png
            plotfile_path_svg = plotfile_path_png.with_suffix('.svg')
            # Save the images and print a message.
            fig.savefig(plotfile_path_png, dpi=Plot_MyDPI, bbox_inches="tight")
            fig.savefig(plotfile_path_svg, dpi=Plot_MyDPI, bbox_inches="tight")
            logandprint("Figure saved to:")
            logandprint(plotfile_path_png)
            logandprint(plotfile_path_svg)
            logandprint('')
    logandprint("--------\n")


if PlotPerFrequency and ShowPlots:
    plt.show()


# Print the time at the end of the program (since this is the last part of it) and the program duration.
endtime = datetime.datetime.now(datetime.timezone.utc)
logandprint("Current time:   {} UTC".format(endtime.replace(tzinfo=None).isoformat(sep=' ')))
logandprint("Program duration: {}".format(endtime-starttime))
logandprint('\n')
