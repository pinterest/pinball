===========================
Pinball Administrator Guide
===========================

.. _README: https://github.com/pinterest/pinball/blob/master/README.rst
.. _here: https://github.com/pinterest/pinball/blob/master/pinball/config/default.yaml#L7
.. _setting: https://github.com/pinterest/pinball/blob/master/pinball/config/default.yaml#L23
.. _workflow_util: https://github.com/pinterest/pinball/blob/master/pinball/tools/workflow_util.py

Please refer README_ to check the installation and quick start. Here, we emphasize how to do Pinball operations.

.. contents::
    :local:
    :depth: 1
    :backlinks: none

Upgrade Pinball
---------------
If you make changes related to Pinball master/worker, you may want to upgrade Pinball to make your changes available. 
Here is the steps to follow.

1. Increment *generation* value in Pinball configuration yaml file. If you don't know where is this configure, 
   please check here_. 

2. Make sure the newly pinball change is available for this upgrade. Sometimes, we need to clean '\*.pyc' files 
   to avoid stale python code.

3. Start a new Pinball worker process ::
  
      python -m pinball.run_pinball -c path/to/pinball/yaml/configuration/file -m workers
  
   Note that, workers threads spawned from the above process will carry a newer (bigger) generation number 
   against the existing running worker threads. 
 
4. Restart Pinball Master

5. Restart Pinball UI

6. Restart Pinball Scheduler

7. Ingest **EXIT** signal into Pinball system. ::
      
      python -m pinball.tools.workflow_util -c path/to/pinball/yaml/configuration/file -f exit      
   
   With this **EXIT** signal in Pinball system, workers with older generation number will die smoothly after they finish
   the current work. 

8. When all worker threads with older generation number are all dead, remove **EXIT** signal from Pinball system. ::

      python -m pinball.tools.workflow_util -c path/to/pinball/yaml/configuration/file -f unexit


Networked Master and Workers
----------------------------
To scale Pinball, it is recommended to deploy Pinball Master and Workers in different machines. 
Assume there are two machines, i.e., their host names are *pinballmaster* and *pinballworker*     

To start the master, ::
      
      [prod@pinballmaster:~]$ python -m pinball.run_pinball -c path/to/pinball/yaml/configuration/file -m master

To start the workers, ::

      [prod@pinballworker:~]$ python -m pinball.run_pinball -c path/to/pinball/yaml/configuration/file -m workers

In the pinball configuration yaml file, we need to configure ::
      
      master_host:            pinballmaster
      master_name:            pinballmaster
The above configuration will guide workers to talk to the right master. 

Add More Workers
-----------------
There are two ways to add more workers in Pinball. 

* Increase the number of workers per worker machine (in the pinball yaml configuration file). ::
      
      workers:                5
The default setting_ for the number of workers per worker machine is 5. After the change, you need to do a Pinball Upgrade. 

* Add more worker machines. 


Ingest/Refresh Workflow Tokens
------------------------------
As discussed in README_, user need to use *workflow_util.py* tool to injest 
their defined workflows and jobs into Pinball system as tokens. As people keeps 
adding more jobs and workflows, a good practice is to **peoridically** run the 
following command to keep update pinball job tokens according to the update-to-date 
workflow configuration. ::
     
      python -m pinball.tools.run_pinball -c path/to/pinball/yaml/configuration/file -f rescheudle

More Tools
-----------
* workflow_util_


