# DXSub

## Quickstart

```shell
  python -m venv venv
  source venv/bin/activate
  pip install -r requirements.txt
  python main.py  --help
  Usage: main.py [OPTIONS]

Options:
  --file_list TEXT          Path to the CSV file containing the file list.
                            [required]
  --max_instances INTEGER   Maximum number of instances to run.  [required]
  --chunk_size INTEGER      Number of files to process per instance.
                            [required]
  --parallel_count INTEGER  Number of parallel jobs to run on each instance.
                            [required]
  --applet_id TEXT          Applet ID to run jobs on.  [required]
  --project_id TEXT         Project ID to associate with jobs.  [required]
  --instance_type TEXT      Type of instance to run jobs on.  [required]
  --output_folder TEXT      Output folder path for processed files.
                            [required]
  --help                    Show this message and exit.


```

## Run DXSub

```shell

  python main.py --file_list file_list.csv --max_instances 4 --chunk_size 6 --parallel_count 3 --applet_id applet-XXXXXXXXXX --project_id project-XXXXXXXX --instance_type mem1_ssd1_v2_x4 --output_folder /dxsub-test/output
```
