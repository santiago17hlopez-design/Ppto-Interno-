# Ppto Interno

## Overview

This project automates the creation and validation of the internal honorarios budget file for different insurance promoters. The process is built in Python and focuses on preparing clean, consistent, and structured data that can later be used for reporting and business analysis.

## Business Problem

The company needs to calculate and validate expected honorarios using payment, production, and rate information from different promoters. Doing this manually can take time and can create errors because each promoter may have different files, formats, and missing information.

This project helps standardize the process and reduce manual work.

## Tools Used

- Python
- pandas
- pathlib
- Excel files
- CSV files
- Parquet files

## What I Did

- Read input files from dynamic promoter folders
- Validated if required files and folders exist
- Cleaned important fields such as agent codes, branch codes, dates, and rate values
- Normalized rates such as `0,1`, `0.1`, or percentage-style values
- Merged rate information from payment files into the internal budget file
- Created fallback logic when files are missing or empty
- Exported the final processed file for later use

## Main Features

- Dynamic paths by promoter
- Validation of missing files
- Data cleaning and formatting
- Rate normalization
- Merge between payment data and internal budget data
- Output generation in a structured folder

## Example Output

The process generates a cleaned file with the necessary honorarios information, including calculated or assigned rate values used for internal validation.

## Conclusion

This project improved the reliability of the honorarios budget process by reducing manual work, standardizing the data structure, and making the process easier to repeat across different promoters.
