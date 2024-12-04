import dxpy as dx

from main import TaskStatus


def list_dx_dir(folder_path, project_id):
    dir_contents = []

    files = dx.find_data_objects(project=project_id, folder=folder_path, recurse=False, describe=True)
    for file_desc in files:
        file_name = file_desc['describe']['name']
        dir_contents.append(file_name)

    return dir_contents


def run_dx_applet(applet_input, run_name, output_folder, instance_type, project_id, applet_id, priority="low"):
    applet_run = dx.DXApplet(applet_id).run(applet_input, name=run_name,
                                            folder=output_folder, instance_type=instance_type,
                                            project=project_id, priority=priority)

    job_id = applet_run.get_id()

    return job_id


def list_dx_applets(applet_id, project_id):
    jobs = dx.find_jobs(executable=applet_id, project=project_id, describe=True)
    jobs_list = [{"job_id": job['id'], "status": job['describe']['state'].lower(), "try": job['describe']["try"]} for job in jobs]
    return jobs_list


def create_dx_cmd(file_list: [TaskStatus], cmd_template: str, extra_vars):
    commands = []
    for file in file_list:
        file_input = {"input_file_name": file.file_name,
                      "output_file_name": file.output_file_name,
                      "input_file_id": file.file_id,
                      **extra_vars}

        commands.append(cmd_template.format(**file_input))

    return commands
