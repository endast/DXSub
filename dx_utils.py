from pathlib import Path

import dxpy as dx


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
    jobs_list = [{"job_id": job['id'], "status": job['describe']['state']} for job in jobs]
    return jobs_list


def create_dx_cmd(file_list, project_id, samples="samples.csv", output_path="~/out/output_files"):
    commands = []
    for file in file_list:
        file_name = Path(file.file_name)
        file_id = file.file_id
        commands.append(
            f'dx download -f {project_id}:{file_id} && bcftools view --output-type u -e "FMT/GQ<=10 | smpl_sum(FMT/LAD)<10" {file_name} | bcftools +fill-tags --output-type u -- --tags HWE | bcftools filter --soft-filter HWE_FAIL -e "INFO/HWE <= 1e-15"  | bcftools view --samples-file {samples} --output-type b --output-file filtered_{file_name.stem.split(".")[0]}.bcf && mv -v filtered_{file_name.stem.split(".")[0]}.bcf {output_path} && rm {file_name}'
        )

    return commands
