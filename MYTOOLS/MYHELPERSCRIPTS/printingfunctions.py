# Imports.
import datetime

from .loggingfunctions import logandprint


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
