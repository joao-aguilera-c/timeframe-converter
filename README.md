# timeframe-converter
Converts any timeframe OHLC data points (e.g. crypto data) to higher timeframes

> timeframe-converter aims to be te go-to library to one who needs fast, controled and time-focused conversion of his candlestick data
> it is built with pandas dataframes in mind and date as string, in order to make it truly universal

## Installation

Checkout the sources and run ``setup.py``:

```
$ python setup.py install
```

### Supported TimeFrames 

```
'1m', '5m', '15m', '30m', '1h', '2h', '3h', '4h', '6h', '12h', '1d', '1w', '1M', '1y'
```

## Usage
To use the converter first you have to import the ``convertcandle`` function

```
from tfConverter import convertcandle
```

### Function args:
```
time(pandas.Series): dataset 'Time' Column.
close(pandas.Series): dataset 'Close' column.
high(pandas.Series): dataset 'High' column.
low(pandas.Series): dataset 'Low' column.
timeframe(str): output candle time Period (see reference above).
fromtime(datetime): begin of conversion.
totime(datetime): end of conversion.
dtformat(str): string of all data input formats
```
