.. :changelog:

History
-------


0.1.1 (2015-03-09)
++++++++++++++++++
* Initial release.


0.2.1 (2015-07-30)
++++++++++++++++++
* Bump minor version to solve some conflict inside Pinterest.


0.2.2 (2015-08-07)
++++++++++++++++++
* Fix workflow status bug if one instance end time is sys.maxint.
* Fix make s3/local file path consistent in LogSaver.
* Add caller name into config_parser is dict based parameter.


0.2.3 (2015-09-01)
++++++++++++++++++
* Fix workflow import util to be more fault tolerant.


0.2.8 (2016-02-12)
++++++++++++++++++
* pydot -> pydot2.
* upgrade qds_sdk and dateutils version.

0.2.9 (2016-03-05)
++++++++++++++++++
* change pkg version spec from '==' to '>=' in setup.py

0.2.10 (2016-03-15)
++++++++++++++++++
* fix pinball_config.py list/tuple issue

0.2.12 (2016-04-09)
++++++++++++++++++
* more precise parser caller
* flexible worker/ui communication protocol
* display worker node in UI

0.2.13 (2017-03-30)
++++++++++++++++++
* tune UI time out value
* make 80 as email port if ui host is provided
* pydot2 -> pydot as pydot fixed incompatibility with pyparsing > 1.5.7
