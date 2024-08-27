from __future__ import annotations

import logging
import time
from enum import Enum as PyEnum

import click
import pandas as pd
from sqlalchemy import Column, String, Enum
from sqlalchemy import create_engine
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session
from sqlalchemy.orm import declarative_base

import dx_utils

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')

WAIT_TIME = 30
DATABASE_URL = "sqlite:///file_tasks.db"
engine = create_engine(DATABASE_URL, echo=False)
Base = declarative_base()


class TaskStatus(PyEnum):
    WAITING = "waiting"
    RUNNING = "running"
    RUNNABLE = "runnable"
    IN_PROGRESS = "in progress"
    DONE = "done"
    FAILED = "failed"
    PARTIALLY_FAILED = "partially failed"
    TERMINATING = "terminating"
    TERMINATED = "terminated"
    DEBUG_HOLD = "debug hold"
    IDLE = "idle"


NON_RUNNING_JOB_STATUSES = [TaskStatus.DONE, TaskStatus.FAILED, TaskStatus.PARTIALLY_FAILED, TaskStatus.TERMINATING,
                            TaskStatus.TERMINATED]
RUNNING_JOB_STATUSES = [TaskStatus.RUNNING, TaskStatus.RUNNABLE, TaskStatus.WAITING, TaskStatus.IDLE]


class FileTask(Base):
    __tablename__ = "tasks"
    file_id = Column(String, primary_key=True, index=True)
    file_name = Column(String, index=True)
    output_file_name = Column(String, index=True)
    job_id = Column(String, index=True)
    status = Column(Enum(TaskStatus), default=TaskStatus.WAITING)


Base.metadata.create_all(engine)


def get_instance_status(project_id, applet_id, status_filter: list = None):
    instances = dx_utils.list_dx_applets(applet_id, project_id)

    if status_filter:
        instances = [i for i in instances if i["status"] in [sf.value for sf in status_filter]]

    return instances


def list_done_files(file_path, project_id):
    files_found = dx_utils.list_dx_dir(file_path, project_id=project_id)
    return files_found


def start_job(file_chunk, parallel_count, session, project_id, applet_id, instance_type, output_folder, cmd_template,
              extra_vars):
    if not extra_vars:
        extra_vars = {}

    commands = dx_utils.create_dx_cmd(cmd_template=cmd_template, file_list=file_chunk, extra_vars=extra_vars)

    applet_input = {
        "command_list": commands,
        "number_jobs": parallel_count
    }

    job_id = dx_utils.run_dx_applet(applet_input=applet_input,
                                    run_name=f"DXSub {len(file_chunk)} {parallel_count} {instance_type}",
                                    output_folder=output_folder, project_id=project_id, applet_id=applet_id,
                                    instance_type=instance_type)

    for file in file_chunk:
        file.job_id = job_id
        file.status = TaskStatus.RUNNING
    session.commit()


def get_file_chunk(chunk_size, session):
    file_chunk = session.query(FileTask).filter(FileTask.status == TaskStatus.WAITING).filter(
        FileTask.job_id.is_(None)).limit(
        chunk_size).all()
    return file_chunk


def setup_file_db(files, session):
    try:
        file_tasks = [
            FileTask(file_name=file["file_name"], file_id=file["file_id"], output_file_name=file["output_file_name"])
            for file in files]
        session.add_all(file_tasks)
        session.commit()
    except IntegrityError as _:
        logging.debug("Files already loaded")
        session.rollback()


def get_waiting_count(session):
    return session.query(FileTask).filter(FileTask.status == TaskStatus.WAITING).count()


def update_file_status(instance_status, file_status, session):
    running_files = session.query(FileTask).filter(
        FileTask.job_id.in_([i["job_id"] for i in instance_status])).filter(
        FileTask.status == TaskStatus.RUNNING).all()

    non_running_jobs = [job["job_id"] for job in instance_status if
                        job["status"] in [s.value for s in NON_RUNNING_JOB_STATUSES]]

    failed_count = 0
    successful_count = 0

    for running_file in running_files:
        if running_file.job_id in non_running_jobs:
            if running_file.output_file_name in file_status:
                running_file.status = TaskStatus.DONE
                successful_count += 1
            else:
                running_file.status = TaskStatus.WAITING
                running_file.job_id = None
                failed_count += 1

    session.commit()
    if successful_count > 0:
        logging.info(f"Successfully loaded {successful_count} files")
    if failed_count > 0:
        logging.info(f"Failed {failed_count} files")


def load_raw_files(file_list_path):
    raw_file_df = pd.read_csv(file_list_path)

    file_list = []

    for f in raw_file_df.itertuples():
        file_list.append({"file_name": f[1], "file_id": f[2], "output_file_name": f[3]})

    return file_list


def main(files, max_instances, chunk_size, paralell_count, applet_id, project_id, instance_type, output_folder,
         cmd_template, extra_vars):
    with Session(engine) as session:
        setup_file_db(files=files, session=session)

        waiting_file_count = get_waiting_count(session)
        running_instance_count = len(get_instance_status(project_id, applet_id, RUNNING_JOB_STATUSES))

        while waiting_file_count > 0 or running_instance_count:
            instance_status = get_instance_status(project_id, applet_id)
            file_status = list_done_files(output_folder, project_id=project_id)
            update_file_status(instance_status, file_status, session)

            running_instance_count = len(get_instance_status(project_id, applet_id, RUNNING_JOB_STATUSES))
            jobs_to_launch = max_instances - running_instance_count
            assert jobs_to_launch >= 0

            logging.debug(f"{running_instance_count} jobs Running")

            logging.debug(f"Can launch {jobs_to_launch} jobs ")
            launched_jobs = 0
            for _ in range(jobs_to_launch):
                file_chunk = get_file_chunk(chunk_size, session)
                if file_chunk:
                    start_job(file_chunk, paralell_count, session, project_id, applet_id, instance_type, output_folder,
                              cmd_template, extra_vars)
                    launched_jobs += 1
                    time.sleep(1)

            if launched_jobs > 0:
                logging.info(f"Launched {launched_jobs} jobs")

            logging.debug(f"Waiting {WAIT_TIME}s ...")
            time.sleep(WAIT_TIME)
            waiting_file_count = get_waiting_count(session)
            running_instance_count = len(get_instance_status(project_id, applet_id, RUNNING_JOB_STATUSES))

    logging.info("All jobs finished")


@click.command()
@click.option('--file_list', required=True, type=str, help='Path to the CSV file containing the file list.')
@click.option('--max_instances', required=True, type=int, help='Maximum number of instances to run.')
@click.option('--chunk_size', required=True, type=int, help='Number of files to process in one chunk.')
@click.option('--parallel_count', required=True, type=int, help='Number of parallel jobs to run.')
@click.option('--applet_id', required=True, type=str, help='Applet ID to run jobs on.')
@click.option('--project_id', required=True, type=str, help='Project ID to associate with jobs.')
@click.option('--instance_type', required=True, type=str, help='Type of instance to run jobs on.')
@click.option('--output_folder', required=True, type=str, help='Output folder path for processed files.')
def cli(file_list, max_instances, chunk_size, parallel_count, applet_id, project_id, instance_type, output_folder):
    raw_files = load_raw_files(file_list)

    # {input_file_name}
    # {output_file_name}
    # {input_file_id}

    cmd_template = '''
    dx download {project_id}:{input_file_id} &&
    bcftools annotate -x ^FORMAT/GT,^FORMAT/GQ,^FORMAT/LAD -Ou {input_file_name} | 
    bcftools +setGT --output-type u -- -t q -i "FMT/GQ<=10 | smpl_sum(FMT/LAD)<7" -n . | 
    bcftools filter --output-type u -e "F_MISSING > 0.1" | 
    bcftools filter --soft-filter HWE_FAIL -e "INFO/HWE <= 1e-15" --output-type b -o {output_file_name} &&
    mv -v {output_file_name} ~/out/output_files &&
    output_file_bcf=$(dx upload ~/out/output_files/{output_file_name} --brief) &&
    echo "Ok uploaded {output_file_name}" &&
    dx-jobutil-add-output output_files "$output_file_bcf" --class=array:file &&
    echo "File done: {output_file_name} $output_file_bcf" &&
    bcftools +setGT --output-type u ~/out/output_files/{output_file_name} -- -t q -i "(FMT/GT=\\"het\\" & (binom(FMT/LAD)<=0.001)) | smpl_sum(FMT/LAD)<10" -n . |
    bcftools filter --output-type v -e "FILTER='HWE_FAIL' | F_MISSING > 0.1" |
    qctool -g - -filetype vcf -og {output_file_name}.bgen &&    
    mv -v {output_file_name}.bgen ~/out/output_files &&
    output_file_bgen=$(dx upload ~/out/output_files/{output_file_name}.bgen --brief) &&
    dx-jobutil-add-output output_files "$output_file_bgen" --class=array:file &&
    echo "File done: {output_file_name}.bgen $output_file_bgen" &&
    rm -v {input_file_name} ~/out/output_files/{output_file_name} ~/out/output_files/{output_file_name}.bgen &&     
    bcftools view --samples-file /cardinal_5k_samples.txt --output-type b step1.bcf -o 5k_{output_file_name} &&    
    mv -v 5k_{output_file_name} ~/out/output_files &&
    output_file_5k=$(dx upload ~/out/output_files/5k_{output_file_name} --brief) &&
    dx-jobutil-add-output output_files "$output_file_5k" --class=array:file &&
    echo "File done: {output_file_name}.bgen $output_file_5k" &&
    rm -v ~/out/output_files/5k_{output_file_name}
    '''.replace("\n", " ").strip()

    extra_vars = {"output_path": output_folder, "project_id": project_id}

    main(
        files=raw_files,
        max_instances=max_instances,
        chunk_size=chunk_size,
        paralell_count=parallel_count,
        applet_id=applet_id,
        project_id=project_id,
        instance_type=instance_type,
        output_folder=output_folder,
        cmd_template=cmd_template,
        extra_vars=extra_vars
    )


if __name__ == '__main__':
    cli()
