import random
import time
from enum import Enum as PyEnum
from pathlib import Path

from sqlalchemy import Column, String, Enum
from sqlalchemy import create_engine
from sqlalchemy import func
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session
from sqlalchemy.orm import declarative_base

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
    job_id = Column(String, index=True)
    status = Column(Enum(TaskStatus), default=TaskStatus.WAITING)


Base.metadata.create_all(engine)


def get_instance_status(session):
    statuses = [TaskStatus.RUNNING, TaskStatus.SUCCESSFUL]

    all_not_sucessfull = session.query(func.max(FileTask.job_id)).filter(FileTask.job_id.is_not(None)).filter(
        FileTask.status != TaskStatus.SUCCESSFUL).group_by(FileTask.job_id).all()

    instance_status = [{"job_id": j[0], "status": random.choice(statuses)} for j in all_not_sucessfull]

    return instance_status


def list_done_files(session):
    files_found = [{"file_id": f"{Path(f.file_id).stem}.bcf"} for f in session.query(FileTask).filter(
        FileTask.status != TaskStatus.SUCCESSFUL).filter(
        FileTask.job_id.is_not(None)).all()]
    orig_files = [{"original_file": f"{Path(f['file_id']).stem}.gz"} for f in files_found]

    if len(orig_files) >= 10:
        return random.choices(orig_files, k=1000)
    else:
        return orig_files


def start_job(file_chunk, paralell_count, session):
    job_id = random.randint(1000, 9000)
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
        file_tasks = [FileTask(file_id=file) for file in files]
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
            if running_file.file_id in [f["original_file"] for f in file_status]:
                running_file.status = TaskStatus.SUCCESSFUL
                successful_count += 1
            else:
                running_file.status = TaskStatus.WAITING
                running_file.job_id = None
                failed_count += 1

    session.commit()
    print(f"Successfully loaded {successful_count} files")
    print(f"Failed {failed_count} files")


def main(files, max_instances, chunk_size, paralell_count):
    with Session(engine) as session:
        setup_file_db(files=files, session=session)

        waiting_count = get_waiting_count(session)

        while waiting_count > 0:
            # Update status
            instance_status = get_instance_status(session)
            file_status = list_done_files(session)
            update_file_status(instance_status, file_status, session)

            running_instance_count = len(get_instance_status(session))
            jobs_to_launch = max_instances - running_instance_count

            print(f" {running_instance_count} jobs Running")

            print(f"Can launch {jobs_to_launch} jobs ")
            launched_jobs = 0
            for _ in range(jobs_to_launch):
                file_chunk = get_file_chunk(chunk_size, session)
                if file_chunk:
                    print(".", end="")
                    start_job(file_chunk, paralell_count, session)
                    launched_jobs += 1
                    time.sleep(0.2)

            print(f"\nLaunched {launched_jobs}")
            print("\nWaiting 2s\n")
            time.sleep(1)
            waiting_count = get_waiting_count(session)

    print("All jobs finished")


if __name__ == '__main__':
    max_instances, chunk_size, paralell_count = 10, 30, 15
    files = [f"file_{f}.vcf.gz" for f in range(1, 1500)]
    main(files, max_instances, chunk_size, paralell_count)
