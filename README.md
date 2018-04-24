# EGCG-Project-Management

[![travis](https://img.shields.io/travis/EdinburghGenomics/EGCG-Project-Management/master.svg)](https://travis-ci.org/EdinburghGenomics/EGCG-Project-Management)
[![Coverage Status](https://coveralls.io/repos/github/EdinburghGenomics/EGCG-Project-Management/badge.svg)](https://coveralls.io/github/EdinburghGenomics/EGCG-Project-Management)
[![GitHub issues](https://img.shields.io/github/issues/EdinburghGenomics/EGCG-Project-Management.svg)](https://github.com/EdinburghGenomics/EGCG-Project-Management/issues)


## GeL delivery
The script `bin/deliver_data_to_gel.py` handles delivery of data via GeL's Rest API and can report on GeL deliveries.
Data upload attempts are stored in a local sqlite database, as well as in GeL's Rest API. The script's usage is as
follows:

### Delivering data
`python bin/deliver_data_to_gel.py --sample_id <sample_id>`

Alternatively, `--user_sample_id` can be used, which will resolve the sample ID from the `User Sample Name` Lims UDF.
This will:

- locate the data to deliver
- check that it has not already been successfully uploaded
- concatenate all relevant `fastq.gz.md5` files into a single md5 report
- create a new delivery record in GeL's Rest API if a delivery attempt has never been made
- rsync the data across
- if the rsync was successful, mark the delivery as successful
- if the rsync failed, mark the delivery as failed
- cleanup intermediate files

Argument options:

- `--force_new_delivery`: always create a new delivery record and re-upload data, even if previously successful
- `--dry_run`: print a summary of what will be delivered, then exit
- `--no_cleanup`: don't clean up files at the end


### Reporting on deliveries
The script can also report on the status of GeL deliveries so far. To do this, it queries each sample in the local
sqlite database against GeL's Rest API. First, it is necessary to ensure that the local database is up to date:

`python bin/deliver_data_to_gel.py --check_all_deliveries`

Then the database can be reported on:

`python bin/deliver_data_to_gel.py --report`

This should give an output like:

```
id   sample_id   external_sample_id  creation_date          upload_state    upload_confirm_date    md5_state    md5_confirm_date       qc_state    qc_confirm_date        failure_reason
1    sample_1    sample_a            2018-04-04 12:00:00    passed          2018-04-04 12:10:00    passed       2018-04-04 12:20:00    passed      2018-04-04 12:30:00    None
2    sample_2    sample_b            2018-04-04 13:00:00    passed          2018-04-04 13:10:00    passed       2018-04-04 13:20:00    passed      2018-04-04 13:30:00    None
```

Note that because the sqlite database is the reference we're using for deliveries, `--check_all_deliveries` will only
update existing delivery records, not add new ones that for some reason exist in GeL's API and not the local database.


### Configuration

- gel_upload
  - delivery_db (local sqlite database)
  - ssh_key (rsync ssh key)
  - username (rsync user name)
  - host (remote GeL data server)
  - dest (target data location on remote server)
  - rest_api
    - host (remote GeL API server)
    - user
    - pswd
- delivery
  - dest (local output data location)
