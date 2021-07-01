"""This is a finantial library useful to convert Candle Data from
financial time series datasets (Open,Close, High, Low, Volume).
It is built on Pandas and Numpy.

.. moduleauthor:: Joao Pedro Aguilera Cardoso

"""

import pandas as pd
from datetime import datetime


def convertcandle(
        time: pd.Series,
        open: pd.Series,
        high: pd.Series,
        low: pd.Series,
        close: pd.Series,
        timeframe: str,
        fromtime: str,
        totime: str,
        dtformat: str) -> pd.DataFrame:
    """OHLC Candle Converter
    Args:
        time(pandas.Series): dataset 'Time' Column.
        close(pandas.Series): dataset 'Close' column.
        high(pandas.Series): dataset 'High' column.
        low(pandas.Series): dataset 'Low' column.
        timeframe(str): output candle time Period (see reference above).
        fromtime(datetime): begin of conversion.
        totime(datetime): end of conversion.
        dtformat(str): string of all data input formats

    timeframe accepted output:
        ['1m', '5m', '15m', '30m', '1h', '2h', '3h', '4h', '6h', '12h', '1d', '1w', '1M', '1y']
    """

    def get_input_tf(time, fmt):
        first = datetime.strptime(time[0], fmt)
        second = datetime.strptime(time[1], fmt)

        tf = (second - first)
        return tf

    def check_tfs(dictframes, timeframe, time, dtformat):
        """this module verify that input is smaller than output."""
        if dictframes[timeframe][1] == 'M':
            dictframes[timeframe][0] = 32
            dictframes[timeframe][1] = 'd'
        if dictframes[timeframe][1] == 'y':
            dictframes[timeframe][0] = 370
            dictframes[timeframe][1] = 'd'

        input_tf = get_input_tf(time, dtformat)
        output_tf = pd.to_timedelta(dictframes[timeframe][0], unit=dictframes[timeframe][1])

        if input_tf >= output_tf:
            raise ValueError("Output timeframe must be bigger than input timeframe.")
        else:
            return input_tf

    def get_candle_times(time, timeframe, fromtime, totime, fmt):
        """This function will generate the time series for the output candle dataframe"""
        time_lst = time.tolist()
        fromtime = datetime.strptime(fromtime, fmt)
        totime = datetime.strptime(totime, fmt)
        for t in time_lst:
            t = datetime.strptime(t, fmt)

            if t >= fromtime:
                if timeframe[1] == 'm' and timeframe[0] < 60:  # minutely tfs
                    if t.minute % timeframe[0] == 0 and t.second == 0:
                        """You found the first candle time"""
                        new_time_lst = []
                        timedelta = pd.to_timedelta(timeframe[0], unit=timeframe[1])
                        while t < totime + timedelta:
                            str_t = datetime.strftime(t, fmt)
                            new_time_lst.append(str_t)
                            t = t + timedelta
                        else:
                            time_df = pd.Series(new_time_lst)
                            return time_df

                if timeframe[1] == 'm' and timeframe[0] > 60:  # hourly tfs
                    if t.hour % (timeframe[0] / 60) == 0 and t.minute == 0 and t.second == 0:
                        """You found the first candle time"""
                        new_time_lst = []
                        timedelta = pd.to_timedelta(timeframe[0], unit=timeframe[1])
                        while t < totime + timedelta:
                            str_t = datetime.strftime(t, fmt)
                            new_time_lst.append(str_t)
                            t = t + timedelta
                        else:
                            time_df = pd.Series(new_time_lst)
                            return time_df
                    pass

                if timeframe[1] == 'd' and timeframe[0] == 1:  # daily tf
                    if t.hour == 0 and t.minute == 0 and t.second == 0:
                        """You found the first candle time"""
                        new_time_lst = []
                        timedelta = pd.to_timedelta(timeframe[0], unit=timeframe[1])
                        while t < totime + timedelta:
                            str_t = datetime.strftime(t, fmt)
                            new_time_lst.append(str_t)
                            t = t + timedelta
                        else:
                            time_df = pd.Series(new_time_lst)
                            return time_df

                if timeframe[1] == 'w' and timeframe[0] < 60:  # weekly tf
                    if t.weekday() == 0 and t.hour == 0 and t.minute == 0 and t.second == 0:
                        """You found the first candle time"""
                        new_time_lst = []
                        timedelta = pd.to_timedelta(timeframe[0], unit=timeframe[1])
                        while t < totime + timedelta:
                            str_t = datetime.strftime(t, fmt)
                            new_time_lst.append(str_t)
                            t = t + timedelta
                        else:
                            time_df = pd.Series(new_time_lst)
                            return time_df

                if timeframe[1] == 'd' and timeframe[0] < 40:  # montly tf
                    if t.day == 1 and t.hour == 0 and t.minute == 0 and t.second == 0:
                        """You found the first candle time"""
                        new_time_lst = []
                        timedelta = pd.to_timedelta(timeframe[0], unit=timeframe[1])
                        while t < totime + timedelta:
                            str_t = datetime.strftime(t, fmt)
                            new_time_lst.append(str_t)
                            if t.month < 12:
                                t = t.replace(month=t.month + 1)
                            else:
                                t = t.replace(month=1, year=t.year + 1)

                        else:
                            time_df = pd.Series(new_time_lst)
                            return time_df

                if timeframe[1] == 'd' and timeframe[0] > 300:  # yearly tf
                    if t.month == 1 and t.day == 1 and t.hour == 0 and t.minute == 0 and t.second == 0:
                        new_time_lst = []
                        timedelta = pd.to_timedelta(timeframe[0], unit=timeframe[1])
                        while t < totime + timedelta:
                            str_t = datetime.strftime(t, fmt)
                            new_time_lst.append(str_t)
                            t = t.replace(year=t.year + 1)
                        else:
                            time_df = pd.Series(new_time_lst)
                            return time_df

    def get_candle_o_h_l_c(out_time, in_time, in_open, in_high, in_low, in_close, fromtime, totime, fmt):
        """The open value of each new candle will be the same value as the open from the last candle"""
        in_df = pd.DataFrame()
        in_df['time'] = in_time
        in_df['open'] = in_open
        in_df['high'] = in_high
        in_df['low'] = in_low
        in_df['close'] = in_close

        out_df = pd.DataFrame(columns=['time', 'open', 'high', 'low', 'close'])
        out_df['time'] = out_time

        open_lst = []
        out_df_time_lst = out_df.values.tolist()
        in_lst = in_df.values.tolist()
        out_cndl_lst = []
        c = 0
        i = 0
        while i < len(in_lst):
            in_row = in_lst[i]
            if datetime.strptime(fromtime, fmt) <= datetime.strptime(in_row[0], fmt):
                if datetime.strptime(in_row[0], fmt) < datetime.strptime(totime, fmt):
                    if in_row[0] == out_df_time_lst[c][0]:
                        time = out_df_time_lst[c][0]
                        open = in_row[1]
                        j = i
                        high_lst = []
                        low_lst = []
                        close = 0
                        while (c + 1) < len(out_df) and j < len(in_lst) and in_lst[j][0] != out_df_time_lst[c + 1][0]:
                            high_lst.append(in_lst[j][2])
                            low_lst.append(in_lst[j][3])
                            close = in_lst[j][4]
                            j += 1
                        out_cndl_lst.append(
                            [
                                time,
                                open,
                                max(high_lst),
                                min(low_lst),
                                close
                            ]
                        )
                        c += 1
                        i = j - 1


                else:
                    out_df = pd.DataFrame(out_cndl_lst, columns=['time', 'open', 'high', 'low', 'close'])
                    return out_df
            i += 1

    dictframes = {'1m': [1, 'm'], '5m': [5, 'm'], '15m': [15, 'm'], '30m': [30, 'm'], '1h': [60, 'm'],
                  '2h': [120, 'm'], '3h': [180, 'm'], '4h': [240, 'm'], '6h': [360, 'm'], '12h': [720, 'm'],
                  '1d': [1, 'd'], '1w': [1, 'w'], '1M': [1, 'M'], '1y': [1, 'y'],
                  }

    input_tf = check_tfs(dictframes, timeframe, time, dtformat)

    new_cndl_times = get_candle_times(time, dictframes[timeframe], fromtime, totime, dtformat)

    new_cndl_o_h_l_c = get_candle_o_h_l_c(new_cndl_times, time, open, high, low, close, fromtime, totime, dtformat)

    return new_cndl_o_h_l_c

