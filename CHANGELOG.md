Changelog for EGCG-Project-Management
=====================================

0.8 (unreleased)
----------------

- Nothing changed yet.


0.7 (2018-05-08)
----------------

- Added customer details and signatures to project report (new config entry)
- Added additional reporting to GeL data delivery and enhancing --force option
- Switched data delivery over to use a delivery step URI
  - removed `--mark_only`, `--project_id`, `--sample_id`
  - added `--process_id`
- Added integration test suite



0.6 (2018-01-17)
----------------

- email sent in HTML rather than plain text
- Project summary examples generated
- Fixes made to project report


0.5 (2017-11-28)
----------------

- Added new script to rsync data to GEL and notify their REST API
- Added recall_sample.py for recalling a sample from tape

0.4 (2017-10-23)
----------------

- Confirm delivery scripts added
- Bug fix project report to use only samples that have been delivered

0.3 (2017-06-22)
----------------
- Added `reference_data` for locating and documenting sources of reference genomes and variant databases
- Updated EGCG-Core to v0.7.1
- Added FluidX barcode support and email reporting to data delivery

0.2.4 (2017-03-30)
------------------
- Fixed a bug where multiple non-human species in a project would crash `project_report`

0.2.3 (2017-03-27)
------------------
- Refactor of automatic review and data deletion
- Update data deletion to use Archive management from EGCG-Core

0.2.2 (2017-01-18)
------------------

0.2.1b (2016-11-29)
-------------------

0.2.1a (2016-08-29)
-------------------

0.2.1 (2016-08-26)
------------------

0.2 (2016-08-26)
----------------
This version adds a script to generate the project report.

0.1 (2016-07-28)
----------------
