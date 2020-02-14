# Decrescendo
### A suite of data analysis tools

## decrescendo.py
### A CSV & XL File Aggregator
This script is for combining multiple .csv and excel files, using pandas to match the headers and group similar tables before exporting the combined data files.

#### Using the Command Line Interface

1. Make sure Python is set as the environmental PATH variable (system-wide, virtual environment, or a portable python distribution command-line such as WinPython)
1. Use the command-line to navigate to the decrescendo directory.
1. Run the script with the following command:  

		python decrescendo.py YOUR_TARGET_PATH_HERE_(IN_QUOTES_IF_THERE_ARE_SPACES)

1. Output files will be returned in the same TARGET PATH set in the initial command and named sequentially. 

*Note: this does not work with XLSB files.*

## SimpleCSVUpload.py
Uploads a .csv file to a server specified in config.ini

## spotify_release_dates.py
Queries the Spotify API to retrieve metadata (release dates, in particular) of a recording. Requires a Spotify developer account (add credentials to config.ini)
