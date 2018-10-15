==================
Pinball User Guide
==================
.. contents::
    :local:
    :depth: 1
    :backlinks: none

Pinball UI
------------
Pinball provides a UI out of the box for users to interact with. The landing page can be seen in the image below. The
default tab in the Pinball UI is a workflow page, which lists all workflows running on this instance. Each record contains
the information for a given workflow. The default page gives us information concerning the workflow owner, name, most recent
status, start time, end time, and duration. By default it is sorted on owner name but can be sorted on any of the fields.

.. image:: images/landing_page.png
   :alt: Landing Page View

Run History
-----------
To get to the run history of a workflow, you simply click on the name of the workflow you'd like to view (or search for
it in the searchbox), and that will take you to another view, giving all the run instances, their respective statuses,
start and end times, as well as durations.

.. image:: images/workflow_history.png
   :alt: Workflow Run History

.. image:: images/workflow_instance_page.png
   :alt: Workflow Instances Run History

Job Logs
--------
To get to a job's execution logs, you need to navigate inside of a workflow instance. Selecting the instance id in
the last picture will take you to the instance run, where we can see a diagram of the entire workflow, the different
jobs associated with the workflow, and a more granular look at run times of these jobs within the workflow.
By selecting the job name, you will be take to the job details page and from there you can see additional links, such as
hadoop application id links, etc, along with this job's execution logs. A job could have run multiple times as seen in
the image below, and each run is associated with its own logs.
Note that the stdout log contains information on which pinball worker the job executed on.

.. image:: images/job_page.png
   :alt: Job View

.. image:: images/job_page_2.png
   :alt: Job View Details

Graph Colors
------------
In the above image, we see different jobs being displayed with different colors. An explanation of the color to its
mapping state can be viewed in this table:

+------------+------------+
| Color      | State      |
+============+============+
| grey       | NEVER_RUN  |
+------------+------------+
| green      | SUCCESS    |
+------------+------------+
| red        | FAILURE    |
+------------+------------+
| blue       | RUNNING    |
+------------+------------+
| orange     | PENDING    |
+------------+------------+
| white      | DISABLED   |
+------------+------------+
| black      | UNKNOWN    |
+------------+------------+


Verifying a Workflow Schedule
-----------------------------
The schedules of all workflows running on a pinball instance can be found at the /schedules endpoint, which is the second
tab in the UI. By selecting on the workflow you'd like to drill into, you'll get information on the next run time, its
recurrence interval, the overrun policy, configuration module, alert notifiers, and more. You can see an example of this
below

.. image:: images/schedule_page.png
   :alt: Workflow Schedule Details

Remember that a workflow object looks as the following::

    'test_workflow': Workflow(
        definition=TEST_JOB,
        final_job=FINAL_JOB,
        schedule={
          'recurrence': '2H',
          'overrun_policy': OverrunPolicy.ABORT_RUNNING,
          'time': '00.30.00.000',
          'max_running_instances': 1,
        },
        notify_emails='bob@pinterest.com'
    )

For the scheduling dictionary, you will need to set the recurrence interval, the overrun policy (more in the next section),
and the start time. There are additional parameters you can pass such as the ``max_running_instances``, ``start_date``, etc.
The recurrence can be any cadence: 2H (2 hours), 1W (1 week), 1Y (1 year), and so on.

Overrun Policy
--------------
The overrun policy is what determines how a new run of a schedule should be brought up. If the overrun policy permits,
run the owned schedule token, otherwise reschedule it at a later time. We say "try to start" because the workflow
instance number is alos limited by the ``max_running_instances`` parameter. Note as well, that the schedule token's
``next_run_time`` should be less than or equal to the current time to trigger the overrun policy to be checked.
The following is an explanation along with an example of each overrun policy:

* START_NEW
    * trigger a new instance of this workflow in parallel to the currently running ones
    * i.e. Always try to start a new instance. No update for token's expirationTime. Only advance to the next run time
      after successfully starting a new workflow instance. Good if execution runs are independent, such as writing data
      to a table partition idempotently, then different executions don't depend on each other.
* SKIP
    * Skip execution if there is already a running instance of this workflow
    * i.e. Always advance to the next run time. Do not update for token's expirationTime. If there are no running instances,
      try to start a new one, otherwise do nothing.
* ABORT_RUNNING
    * Abort the older running instances and start a new instance of this workflow
    * i.e. There are no updates for the token's expirationTime. If abort signal is net, try to start a new workflow instance,
      otherwise do nothing. It will only advance to the next run time after successfully starting a new workflow instance.
* DELAY
    * Hold off on beginning th execution until the previous runs have finished
    * i.e. If there are no running instances, try to begin a new one, otherwise extend token's expirationTime. Only advance
      to the next run time after successfully starting a new workflow instance.
* DELAY_UNTIL_SUCCESS
    * Delay the execution until the previous one succeeds
    * i.e. If there are no running workflow instances and the last instance is SUCCESS, try to begin a new run, otherwise
      extend the token's expirationTime. Only advance to the next run time after successfully starting a new workflow instance.

Updating a Workflow Schedule
----------------------------
To update a workflow schedule, you will need to modify the object definition's schedule dictionary with your new schedule,
and for it to get picked up properly, you will need to restart the scheduler and worker processes.
Be sure to be aware that if the current time has passed your new scheduling interval, it may be that a run gets skipped.
In general, it is best to be safe about when you'd like to change the schedule of a workflow, to prevent missed runs.

Unscheduling a Workflow
-----------------------
To remove a workflow from being picked up by the pinball scheduler, there are two steps to be taken:
1. Comment out or remove the configuration code that put the workflow in Pinball
2. Go to the schedule page in Pinball UI for the specific workflow and click unschedule in the action dropdown:

.. image:: images/unschedule_workflow.png
   :alt: Unschedule Workflow

Manually Triggering a Workflow
------------------------------
Go to the schedule view in the Pinball UI, select the workflow at hand, and select the start button in the action drop down:

.. image:: images/manual_start_workflow.png
   :alt: Manually Start Workflow

Workflow Instance Actions
-------------------------
We may want to take other actions on workflows such as stopping a job from running or retrying a specific failure. To do
this we need to get to the workflow instance page, clicking the action drop down, and that action will get performed on the job.
A list of actions and their explanations are in the table below:

.. image:: images/action_page.png
   :alt: Workflow Actions

+------------+-------------------------------------------------------------------------------------+
| Action     | Description                                                                         |
+============+=====================================================================================+
| Drain      | Finish all currently running jobs, and prevent new ones from starting.              |
+------------+-------------------------------------------------------------------------------------+
| Undrain    | Stop the drain process.                                                             |
+------------+-------------------------------------------------------------------------------------+
| Abort      | Abort this workflow instance.                                                       |
+------------+-------------------------------------------------------------------------------------+
| Unabort    | Cancel abort. No-op if run on a finished workflow.                                  |
+------------+-------------------------------------------------------------------------------------+
| Retry      | Retry failed jobs.                                                                  |
+------------+-------------------------------------------------------------------------------------+
| Poison     | Run the selected jobs and all its direct/indirect dependents.                       |
+------------+-------------------------------------------------------------------------------------+
| Disable    | Don't run the selected jobs, mark as success immediately after it becomes runnable. |
+------------+-------------------------------------------------------------------------------------+
| Enable     | Enable disabled jobs.                                                               |
+------------+-------------------------------------------------------------------------------------+

Passing Parameters Between Jobs
-------------------------------
Pinball allows an upstream job to pass attributes to downstream dependencies. The executor has the ability to interpret
specially formatted job output. For example, printing ``PINBALL:EVENT_ATTR:akey=aval`` to the job output will embed an
akey=aval pair in the job output event. Output events traverse along job output edges and they are accessible by downstream
jobs. Event attributes are used to customize the job command line. To show this, the command ``echo %(akey)s`` will be translated
to ``echo aval`` by the job executor. Below is an example of this::
    PARENT_JOB = CommandJobTemplate('parent', 'echo PINBALL:EVENT_ATTR:akey=avl')
    # Note we escape the % with %% so the workflow parser evaluates it correctly
    CHILD_JOB = CommandJobTemplate('child', 'echo "child: %%(akey)s"')
    # The previous commands will produce: echo PINBALL:EVENT_ATTR:akey=aval child: aval

Creating a Workflow
--------------------
To get an idea of how to build workflows and jobs, you should take a look at the
`example_repo <https://github.com/pinterest/pinball/tree/master/tutorial/example_repo>`_. There are simple python job
examples, hadoop batch processing examples, bash command job examples, and more.