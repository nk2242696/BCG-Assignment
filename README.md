# BCG-Analytics

## About
This Spark Job is designed to support 8 Analytics query pattern against 6 given tables
* CHARGES
* DAMAGES
* ENDORSE
* PRIMARYPERSON
* RESTRICT
* UNITS

### Hierarchy
* MODEL (6 files each specific for reading data from 1 file type)
* RESOURCES (6 sample input csv files)
* OUTPUT (data from 8 analytics query saved in output)
* IMAGES (output of 8 query captured in snippet )
* main.py (Driver process to execute Spark Job)

The application includes functionality, to update input and output filepath using configs file :
1. Load data 
    1. Update source_data_path in config.json {Default-> resources}
    
1. Export data
    1. The data can be exported in CSV format
    1. Update output_data_path in config.json {Default-> output}



## How to use

1. All the dependency are included as zip files
2. Make data_reader as sources root
3. python3 main.py
