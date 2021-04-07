# Downloading and extracting archival data

All commands listed assume your current working directory is `data/`, not the
project root.

1. Download the raw dataset from figshare
   ```
   curl -L https://ndownloader.figshare.com/files/25160942 -o dataset.tar.gz
   ```

2. Extract the dataset

   ```
   tar -xzvf dataset.tar.gz
   ```

3. Move the dataset exported files to their expected locations

   ```
   mv export clean
   ```

4. Remove the download tar.gz file
    ```
    rm dataset.tar.gz
    ```

4. Validate the structure of your data directory. Your data directory should look like this:

   ```
    clean
    ├── flows
    │   ├── p2p_TM_DIV_none_INDEX_start
    │   │   ├── _common_metadata
    │   │   ├── _metadata
    │   │   └── part.0.parquet
    │   └── typical_fqdn_org_category_local_TM_DIV_none_INDEX_start
    │       ├── _common_metadata
    │       ├── _metadata
    │       ├── part.0.parquet

    ...

    │       └── part.281.parquet
    ├── log_gaps_TM.parquet
    ├── README.md
    ├── transactions_TM.parquet
    └── user_active_deltas.parquet
    ```

# Descriptions of archival files

* **flows**: All flows are stored as dask-generated parquet files. To load these
  files, ask dask to load the *entire subdirectory*, not each specific file, and
  it will load each part parquet file into a separate partition. The flows
  directory contains two subdirectories.

    - `typical_fqdn_org_category_local_TM_DIV_none_INDEX_start` contains all of
      the typically structured flows in the network, where a single user
      interacts with an internal or external domain. This is the majority of the
      dataset.
    - `p2p_TM_DIV_none_INDEX_start` contains the rare flows between two end-user
      ip addresses in the network. This dataset has two user columns, instead of
      a user and destination column.

* **transactions_TM.parquet**: This file is a pandas single parquet file
  containing all financial transactions in the network during the study interval.

* **user_active_deltas.parquet**: This is a rendered pandas single parquet file
  containing the amount of time each user was active and/or online in the
  network during the study interval. Importantly this file was generated with
  information from outside the archival dataset, and counts users as active at
  the start of the study who were already active in the network before the start
  of the study dataset window.

* **log_gaps_TM.parquet**: This is a rendered pandas parquet file containing all
  points where no logs were being recorded at all. This usually indicates some
  kind of network outage, since it is extremely rare that no flows are in
  progress (including passive background flows) for an entire 20 minute
  interval.

# Details of original raw data

The original raw dataset was processed to aggregate together the individual
uploaded data dumps, assign domains, categories, and organizations, and group
infrequently visited organizations into an "other" organization.

## Steps taken to clean data

1. Remove nil characters from the transactions log. This is likely due to nodejs
   buffering the file and then having the power yanked out from underneath it.

2. Sanity check the transaction log and flowlogs have overlapping sets of user
   ids. This was an issue at first, but flowlogs have been re-exported now with
   matching keys after re-coordination with the operator.

3. Separate out anomalous flows. Some flows made it onto the tunnel interface
   with bogus source addresses. These may have been spoofed as part of a
   short-lived DOS attack.

4. Separate out peer-to-peer flows.

5. Canonicalise the direction of the flows and store in a specific flow type.

6. Convert IP addresses objects to strings (to allow native serialization and
   comparison in dask)

7. Concatenate the flowlogs retrieved at different times into one consolidated
   log.

8. Remove duplicates from the consolidated log.

9. Add FQDN information where available, either from a previous DNS request by
   the user, or if not available, via a reverse DNS lookup.

10. Add category and organization information to each flow.

11. Re-map rare organizations to an "other" entry for the ip, fqdn, and org.

## Raw data layout (not relevant for external users)

Data is contained in 2 main directories, the clean directory and the originals
directory. The originals files are the raw files from the field, and have some
issues. These issues are addressed in the `canonicalize_data.py` module which
generates the cleaned and consolidated dataset in the clean directory.

Additionally, outside the canonicalize data function, there are also cleaning
scripts to shift the data into the local timezone and trim the datasets to a
consistent range and datatype. These scripts are not well-maintained and need to
be run somewhat manually and interactively. First run canonicalize, then shift,
then trim. The scripts can be found in the `cleaning` directory