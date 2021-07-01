from setuptools import setup, find_packages

setup(
    name='timeframe-converter',
    version='0.1',
    packages=find_packages(),
    license='MIT',
    description='Converts any timeframe OHLC data points (e.g. crypto candlestick data) to higher timeframese',
    long_description=open('README.txt').read(),
    install_requires=['pandas'],
    url='https://github.com/joao-aguilera-c/timeframe-converter',
    author='João Aguilera',
    author_email='jpacardoso@yahoo.com'
)
