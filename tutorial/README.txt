0. Install Pinball pypi package.

1. clone the tutorial folder under some path, say '/mnt'

2. Configure it for your usage:
  - /mnt/tutorial/example_repo/tutorial.yaml
  - /mnt/tutorial/example_repo/job_templates.py
  - /mnt/tutorial/example_repo/workflows.py

3. cd /mnt

4. start pinball master/ui/scheduler/workers
PYTHONPATH=. python -m pinball.run_pinball -c tutorial/example_repo/tutorial.yaml -m master
PYTHONPATH=. python -m pinball.run_pinball -c tutorial/example_repo/tutorial.yaml -m ui
PYTHONPATH=. python -m pinball.run_pinball -c tutorial/example_repo/tutorial.yaml -m scheduler
PYTHONPATH=. python -m pinball.run_pinball -c tutorial/example_repo/tutorial.yaml -m workers

5. ingest workflow tokens into Pinball system.
PYTHONPATH=. python -m pinball.tools.workflow_util -c tutorial/example_repo/tutorial.yaml -f reschedule
