import os


class Paths:

    def __init__(self, project_dir):
        self.project_dir = project_dir

    @property
    def vaccinations(self):
        return os.path.join(self.project_dir, "scripts", "scripts", "vaccinations")

    @property
    def output_dir_get_data(self):
        return os.path.join(self.vaccinations, "output")

    @property
    def in_us_states(self):
        return os.path.join(self.vaccinations, "us_states", "input")

    def out_tmp(self, location):
        return os.path.join(self.output_dir_get_data, f"{location}.csv")

    def out_tmp_man(self, location):
        return os.path.join(self.output_dir_get_data, "by_manufacturer", f"{location}.csv")
