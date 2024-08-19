import time
from enum import Enum as PyEnum
from pathlib import Path

import pandas as pd
from sqlalchemy import Column, String, Enum
from sqlalchemy import create_engine
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session
from sqlalchemy.orm import declarative_base

import dx_utils

WAIT_TIME = 30
DATABASE_URL = "sqlite:///file_tasks.db"
engine = create_engine(DATABASE_URL, echo=False)
Base = declarative_base()


class TaskStatus(PyEnum):
    WAITING = "Waiting"
    RUNNING = "Running"
    SUCCESSFUL = "Successful"
    FAILED = "Failed"


class FileTask(Base):
    __tablename__ = "tasks"
    file_id = Column(String, primary_key=True, index=True)
    file_name = Column(String, index=True)
    job_id = Column(String, index=True)
    status = Column(Enum(TaskStatus), default=TaskStatus.WAITING)


Base.metadata.create_all(engine)


def get_instance_status(project_id, applet_id, status_filter: TaskStatus = None):
    instances = dx_utils.list_dx_applets(applet_id, project_id)

    if status_filter:
        instances = [i for i in instances if i["status"] == status_filter]

    return instances


def list_done_files(file_path, project_id):
    files_found = dx_utils.list_dx_dir(file_path, project_id=project_id)

    orig_files = [{"original_file": f"{Path(f).stem}.gz"} for f in files_found]

    return orig_files


def start_job(file_chunk, parallel_count, session, project_id, applet_id, instance_type, output_folder):
    commands = dx_utils.create_dx_cmd(file_list=file_chunk, project_id=project_id)

    applet_input = {
        "command_list": commands,
        "number_jobs": parallel_count
    }

    job_id = dx_utils.run_dx_applet(applet_input=applet_input,
                                    run_name=f"From jupyter {parallel_count} {instance_type}",
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
        file_tasks = [FileTask(file_name=file["file_name"], file_id=file["file_id"]) for file in files]
        session.add_all(file_tasks)
        session.commit()
    except IntegrityError as _:
        print("Files already loaded")
        session.rollback()


def get_waiting_count(session):
    return session.query(FileTask).filter(FileTask.status == TaskStatus.WAITING).count()


def update_file_status(instance_status, file_status, session):
    running_files = session.query(FileTask).filter(
        FileTask.job_id.in_([i["job_id"] for i in instance_status])).filter(
        FileTask.status == TaskStatus.RUNNING).all()

    failed_count = 0
    successful_count = 0

    for running_file in running_files:
        if running_file.job_id in [job["job_id"] for job in instance_status if job["status"] != TaskStatus.RUNNING]:
            if running_file.file_name in [f["original_file"] for f in file_status]:
                running_file.status = TaskStatus.SUCCESSFUL
                successful_count += 1
            else:
                running_file.status = TaskStatus.WAITING
                running_file.job_id = None
                failed_count += 1

    session.commit()
    print(f"Successfully loaded {successful_count} files")
    print(f"Failed {failed_count} files")


def main(files, max_instances, chunk_size, paralell_count, applet_id, project_id, instance_type, output_folder):
    file_output_path = '/snakemake-test/output/chr1'

    with Session(engine) as session:
        setup_file_db(files=files, session=session)

        waiting_count = get_waiting_count(session)
        running_instance_count = len(get_instance_status(project_id, applet_id, TaskStatus.RUNNING))

        while waiting_count > 0 or running_instance_count:
            instance_status = get_instance_status(project_id, applet_id)
            file_status = list_done_files(file_output_path, project_id=project_id)
            update_file_status(instance_status, file_status, session)

            running_instance_count = len(get_instance_status(project_id, applet_id, TaskStatus.RUNNING))
            jobs_to_launch = max_instances - running_instance_count
            assert jobs_to_launch > 0

            print(f"{running_instance_count} jobs Running")

            print(f"Can launch {jobs_to_launch} jobs ")
            launched_jobs = 0
            for _ in range(jobs_to_launch):
                file_chunk = get_file_chunk(chunk_size, session)
                if file_chunk:
                    print(".", end="")
                    start_job(file_chunk, paralell_count, session, project_id, applet_id, instance_type, output_folder)
                    launched_jobs += 1

            print(f"\nLaunched {launched_jobs}")
            print(f"\nWaiting {WAIT_TIME}s\n")
            time.sleep(WAIT_TIME)
            waiting_count = get_waiting_count(session)

    print("All jobs finished")


def load_raw_files(file_list_path):
    raw_file_df = pd.read_csv(file_list_path)

    file_list = []

    for f in raw_file_df.itertuples():
        file_list.append({"file_name": f[1], "file_id": f[2]})

    return file_list


if __name__ == '__main__':
    PROJECT_ID = 'project-GkZfY7QJ704p8J8vfZ89gj6k'
    APPLET_ID = 'applet-GpjJzG8J704V240FYqP0gPx0'
    instance_type = "mem1_ssd1_v2_x4"
    output_folder = "/snakemake-test/output"

    max_instances, chunk_size, paralell_count = 2, 6, 3

    raw_files = load_raw_files("chr1_files.csv")

    main(files=raw_files, max_instances=max_instances, chunk_size=chunk_size, paralell_count=paralell_count,
         applet_id=APPLET_ID, project_id=PROJECT_ID, instance_type=instance_type, output_folder=output_folder)
