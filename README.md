# Tim Dataset Utility

This repository includes a set of scripts and functions for processing and analyzing this dataset: [A multi-source dataset of urban life in the city of Milan and the Province of Trentino](https://www.nature.com/articles/sdata201555). 

In the dataset the position of the base stations is hidden, while data is collected over square cells with a size of $235 \times 235$ $m^2$. Inside each cell, traffic volume is aggregated and anonymized every ten minutes. A new trace is generated every time a user receives or sends SMS/calls or an Internet connection starts / ends.
We then combine the position of the grid cells with the estimated position of real base stations obtained from a public dataset [OpenCellId](https://opencellid.org/#zoom=16&lat=37.77889&lon=-122.41942).

The following pipeline allows aggregating traces based on the position obtained from OpenCellId.

## Install

```shell
git clone <repo_link>
cd tim-dataset-utility
pip install -r requirements.txt
```

## Scripts

The scripts available are 3:

- `chunks-pipeline`: the first step download all the chunks, but then it aggregates the data by hour, saves the aggregated chunks, and removes the full chunks for space-saving
- `bs`: a pipeline for downloading all the base stations inside a grid of cells. After the download, it saves all the base station geojson into MongoDB for faster and more efficient querying later on. As the last step, the pipeline creates a macro base station for each cell. (Note: this script requires a MongoDB instance, see the MongoDB section)

The aggregation performed by the ``bs`` script is done as follows:

- for each cell, we look for the closest base stations
- if no BS is inside the cell the closest will be the only BS for that cell.
- If at least 2 BS are inside the same cell, they are aggregated together and the information about the number of BS aggregated is kept to allow a capacity proportional to the number of BS aggregated.

All the scripts can be started by:

```shell
python dataset.py <script_name>
```

Use the command ``-h`` or `--help` for the full list of arguments available for each script.

## Additional functionalities

In addition to the scripts above, the repository includes some methods for visualizing the data on the map through the library Plotly.

In the file ``quick_test.py`` there are examples of usage for all of them.

### MongoDB

The ``bs`` script requires a MongoDB instance, for simplicity and for reason of performance (it allows local calls), we provide a docker-compose that can be used for starting a container with MongoDB. In addition, to the MongoDB instance, the docker-compose starts an instance of mongo-express for allowing to browse the data inserted in the DB.

For starting the docker-compose:

```shell
docker-compose up -d
```

For connecting to MongoDB, independently of the way used for having an instance of it, it is necessary to create a ``.env`` with the environment variables used for instantiating the connection. The repository provides a sample file (`.env.sample`) with all the variables and with their default values (they match the current configuration of the docker-compose).

Note: in case you changed any configuration you need to change them also in the ``.env`` file

## Requirements

- python 3 and pip
- docker and docker-compose

### Dataset file requirements

- Milan and Province of Trento metadata file
- Milan and Province of Trento grid geojson file

Both the files are already provided in the ``data`` folder.

In addition, in the ``data/milan`` folder the results of the `bs` are already generated and stored.

## Files Produced by the scripts

Many of the produced files contain indications about the BS technology used for generating the file. The information is added as the last portion of the file name. For example, if the file used only LTE base stations the file name will be ``some-name-LTE``. If it used LTE and UMTS it would be `some-name-LTE-UMTS`.

For the purpose of this documentation, we will refer to `<BS-TYPES>` as an indication in the file name related to the BS technology used for generating the file. 

### bs pipeline

- `cell_base_stations_mapped-<BS-TYPES>.csv`:  for each cell, it maps the cell to all the base stations in its coverage area. The number of BS in the coverage area is 1 if the closest BS is not inside the cell, otherwise, we have one row for each BS mapped to a certain cell.

Header: 
```
bs_id,type,range,created,lng,lat,cellId,distance
```

`distance` is the distance between the cell and the closest BS

- `cell_base_stations_aggregated-<BS_TYPES>.csv`: some as `cell_base_stations_mapped-<BS-TYPES>.csv`, but here the rows are aggregated when more BSs are inside a single cell, the count is added as a new column. The number of rows is equal to the number of cells.

Header:

```
type,lng,lat,cellId,distance,n_base_stations,aggregated_bs_id
```

`n_base_stations` shows the number of BSs aggregated

- `aggregated_bs_data-<BS_TYPES>.csv`: it contains the information for each aggregated BS from the `cell_base_stations_aggregated-<BS_TYPES>.csv` file. It contains one row for each BS.

Header:

```
aggregated_bs_id,type,n_base_stations,lng,lat
```

### chunks-pipeline pipeline

- `processed-chunks/internet-<city>-<date>.csv`: for each chunk of the dataset a file is created containing the internet demand aggregated by cell, hour, and weekday

Header:

```
hour,weekday,cellId,internet,idx
```

``idx`` is an id created by summing the hour of the row plus the weekday multiplied by 24. It gives a unique value for each different (hour, weekday) tuple

- `aggregated-chunks/aggregated-internet-<city>-<date>.csv`: provides a file for each dataset chunk. The rows contain for each aggregated BS the internet demanded every hour and weekday

Header: 

```
hour,weekday,idx,aggregated_bs_id,internet
```

- `aggregated-internet-<city>-minimal-data-<BS_TYPES>.csv`: it merges all the chunks from `aggregated-chunks/aggregated-internet-<city>-<date>.csv`

Header:

```
hour,weekday,idx,aggregated_bs_id,internet
```

- `aggregated-internet-<city>-full-data-<BS_TYPES>.csv`: it merges all the chunks from `aggregated-chunks/aggregated-internet-<city>-<date>.csv`, but it all keeps all the information about the aggregated BSs. The information is the same provided in the `aggregated_bs_data-<BS_TYPES>.csv` but they are added for each trace.

Header:

```
hour,weekday,idx,aggregated_bs_id,type,lng,lat,n_base_stations,internet
```
