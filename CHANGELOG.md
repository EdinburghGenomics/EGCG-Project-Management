Changelog for EGCG-Project-Management
=====================================

0.11 (unreleased)
-----------------

- Nothing changed yet.


0.10.1 (2019-04-29)
-------------------

- Removed old project report
- Removed old aggregation usage
- Pointing user to attached project report in delivery email



0.10 (2019-02-28)
-----------------

- Project report appendix tables are now fit dynamically, depending on their content
- Project report code refactor, modifying project_information.py, project_report_latex.py, pylatex_ext.py and utils.py
- Fixed bug when performing final data deletion by filtering out "Undetermined" sample IDs
- Project report now supports different analysis types
- Data deletion now removes sample folder
- Provides more accurate feedback if data deletion fails, detailing only the cause of failure
- Project report is now generated using Latex and converting it to PDF before exporting
- lims.get_processes now uses the appropriate parameter
- Try/except catches cases where human has more than one analysis type


0.9 (2018-11-30)
----------------

- `reference_data.py` now downloads reference genomes from Ensembl and EnsemblGenomes. It also prepare the data, upload metadata and perform simple validation.
- `delete_data.py` has a new mode for deletion of orphan filestub in the DMF filesystem
- Fix bug in data deletion where fluidX samples where ignored in the delivered folder
- New report run script that generate email to report successes/failures/repeats to the lab 


0.8 (2018-08-08)
----------------

- New mode in delete_data.py to perform final data deletion.
- detect_sample_to_delete.py has a mode to detect sample ready for final deletion
- Raw data deletion now select run that have been reviewed 14 days ago. It also uses notification in case of a crash

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
