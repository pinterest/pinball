============
Pinball
============

.. image:: https://travis-ci.org/pinterest/pinball.svg
    :target: https://travis-ci.org/pinterest/pinball

Pinball is a scalable workflow manager.

.. contents::
    :local:
    :depth: 1
    :backlinks: none

Design Principles
----------------
* **Simple**: based on easy to grasp abstractions
* **Extensible**: component-based approach
* **Transparent**: state stored in a readable format
* **Reliable**: stateless computing components
* **Scalable**: scales horizontally
* **Admin-friendly**: can be upgraded without aborting workflows
* **Feature-rich**: auto-retries, per-job-emails, runtime alternations, priorities, overrun policies, etc.


Installation
----------------------
If you haven't already installed *libmysqlclient-dev*, *graphviz*. Please install them, e.g., ::

   $ sudo apt-get install libmysqlclient-dev
   $ sudo apt-get install graphviz

If you want to install *Pinball* through pypi package, please do ::

  $ sudo pip install --allow-unverified pydot pydot==1.0.28
  $ sudo pip install pinball

Pinball uses mysql as persistent storage. Please also make sure mysql is available, and properly configured.


Quick Start
----------------------

Start Pinball
~~~~~~~~~~~~~
Once Pinball is installed either through pypi package installation or source code clone, we are ready to run it. There are four important components in Pinball.

* **Master**: A frontend to a persistent state repository with an interface supporting atomic job token updates. To start master, ::

  $ python -m pinball.run_pinball -c path/to/pinball/yaml/configuration/file -m master

* **UI**: A service reading directly from the storage layer used by the Master. To start UI, ::

  $ python -m pinball.run_pinball -c path/to/pinball/yaml/configuration/file -m ui

* **Scheduler**: Scheduler is responsible for running workflows on a schedule. To start scheduler, ::

  $ python -m pinball.run_pinball -c path/to/pinball/yaml/configuration/file -m scheduler

* **Worker**: A client of the Master. To start worker, ::

  $ python -m pinball.run_pinball -c path/to/pinball/yaml/configuration/file -m workers


Configure Pinball
~~~~~~~~~~~~~~~~~
In order to start Pinball, user needs to provide a pinball configuration file. A sample pinball configuraiton can be retrived at here_.

.. _here: https://github.com/pinterest/pinball/blob/master/pinball/config/default.yaml

There are a few parameters to configure. For example:

* MySQL db configuration ::

   databases:
        default:
            ENGINE:       django.db.backends.mysql
            NAME:         pinball
            USER:         flipper
            PASSWORD:     flipper123
            HOST:         127.0.0.1
            PORT:         "3306"
        pinball.persistence:
            ENGINE:       django.db.backends.mysql
            NAME:         pinball
            USER:         flipper
            PASSWORD:     flipper123
            HOST:         127.0.0.1
            PORT:         "3306"

* Pinball UI configuration ::

   ui_host:                  pinball
   ui_port:                  8080

.. _example: https://github.com/pinterest/pinball/blob/master/pinball_ext/examples/workflows.py
.. _parser: https://github.com/pinterest/pinball/blob/master/pinball_ext/workflow/parser.py
* Application Configuration ::

    parser:                    pinball_ext.workflows.parser.PyWorkflowParser
      
  *parser* tells Pinball how to interpreate your defined workflow and jobs. The above configuration links to an Python parser_ provided by Pinball.
  You can also provide your own parser to intepretate your own definition of workflow and jobs. Please check the tutorial for details. ::

    parser_params:
      workflows_config:       pinball_ext.examples.workflows.WORKFLOWS
      job_repo_dir:           "~"
      job_import_dirs_config: pinball_ext.examples.jobs.JOB_IMPORT_DIRS
  
  *parser_params* will be taken by *parser*. Name of the variable that stores workflows config is *workflows_config*; 
  root dir of the repo that stores all user defined jobs is stored at *job_repo_dir*; *job_import_dirs_config* keeps list of 
  dirs where job class should be imported from.   
      

* Email configuration ::

    default_email:              your@email.com
   
  *default_email* configures default sender of email service of Pinball.    

   
Use Pinball
~~~~~~~~~~~
After starting Pinball with the proper configuration, user can access Pinball at *pinball:8080*. 
You may find there is no workflow or jobs listed in Pinball UI when you first start Pinball. To import your workflow into Pinball, 
do the following command. ::
    
    python -m pinball.tools.workflow_util -c path/to/pinball/yaml/configuration/file -f reschedue

After this, you should be able to see your workflows in Pinball UI. They will be scheduled and run according to the specified schedules. 

.. figure:: https://github.com/pinterest/pinball/blob/master/instance_view.png
   :alt: Workflow instance view

Detailed Design
------------- 
Design details are available in `Pinball Architecture Overview <https://github.com/pinterest/pinball/blob/master/ARCHITECTURE.rst>`_

User Guide
-----------------
Detail user guide is available in `Pinball User Guide <https://github.com/pinterest/pinball/blob/master/USER_GUIDE.rst>`_

Admin Guide
------------------
Administrator guide is available in `Pinball Administrator Guide <https://github.com/pinterest/pinball/blob/master/ADMIN_GUIDE.rst>`_

License
-------
Pinball is distributed under `Apache License, Version 2.0 <http://www.apache.org/licenses/LICENSE-2.0.html>`_.

Maintainers
----------
* `Pawel Garbacki <https://github.com/pgarbacki>`_
* `Mao Ye <https://github.com/MaoYe>`_
* `Changshu Liu <https://github.com/csliu>`_

Contributing
-----------
* `Contributors <https://github.com/pinterest/pinball/blob/master/AUTHORS.rst>`_
* `How to contribute <https://github.com/pinterest/pinball/blob/master/CONTRIBUTING.rst>`_


Help
-----
If you have any questions or comments, you can reach us at `pinball-users@googlegroups.com <https://groups.google.com/forum/#!forum/pinball-users>`_.

