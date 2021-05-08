import yaml
import os

from vax.cmd.get_data import modules_name, modules_name_batch, modules_name_incremental, country_to_module
from vax.cmd._parser import _parse_args


def get_config():
    args = _parse_args()
    return ConfigParams.from_args(args)


class ConfigParamsStep(object):
    def __init__(self, adict):
        self._dict = adict
        self.__dict__.update(adict)

    def __str__(self):
        def _is_secret(name):
            secret_keys = ["id", "token", "credentials", "credential", "secret"]
            return any(x in name for x in secret_keys)

        return f"\n".join([f"* {k}: {v}" for k, v in self._dict.items() if not _is_secret(k)])


class ConfigParams(object):

    def __init__(self, config_file, parallel, njobs, countries, mode, display):
        self.config_file = config_file
        self._config = self._load_yaml()
        self.parallel = parallel
        self.njobs = njobs
        self.countries = countries
        self.mode = mode
        self.display = display
        self.project_dir = self._get_project_dir_from_config()

    @classmethod
    def from_args(cls, args):
        return cls(
            config_file=args.config,
            parallel=args.parallel,
            njobs=args.njobs,
            countries=_countries_to_modules(args.countries),
            mode=args.mode,
            display=args.show_config,
        )

    @property
    def config_file_exists(self):
        return os.path.isfile(self.config_file)

    def _get_project_dir_from_config(self):
        try:
            return self._config["global"]["project_dir"]
        except KeyError:
            raise KeyError("Missing global.repo_dir variable in config.yaml")

    def _load_yaml(self):
        if self.config_file_exists:
            with open(self.config_file) as file:
                config = yaml.load(file, Loader=yaml.FullLoader)
            return config
        return {}

    @property
    def GetDataConfig(self):
        """Use `_token` for variables that are secret"""
        return ConfigParamsStep({
            "output_dir": self._return_value("get-data", "output_dir", "output"),
            "parallel": self._return_value("get-data", "parallel", self.parallel),
            "njobs": self._return_value("get-data", "njobs", self.njobs),
            "countries": self._return_value("get-data", "countries", self.countries),
            "greece_api_token": self._return_value("get-data", "greece_api_token", None),
        })

    @property
    def ProcessDataConfig(self):
        """Use `_token` for variables that are secret"""
        return ConfigParamsStep({
            "skip_complete": self._return_value("process-data", "skip_complete", []),
            "skip_monotonic_check": self._return_value("process-data", "skip_monotonic_check", []),
            "google_credentials": self._return_value("process-data", "google_credentials", None),
            "google_spreadsheet_vax_id": self._return_value("process-data", "google_spreadsheet_vax_id", None),
        })

    def _return_value(self, step, feature_name, feature_from_args):
        try:
            v = self._config["pipeline"][step][feature_name]
            if v is not None:
                return v
            else:
                return feature_from_args
        except KeyError:
            return feature_from_args

    def __str__(self):
        if self.config_file_exists:
            s = f"CONFIGURATION PARAMS:\nfile: {self.config_file}\n\n"
            s += "*************************\n"
        else:
            s = f"CONFIGURATION PARAMS:\nNo config file\n\n"
            s += "*************************\n"
        if self.mode == "get-data":
            s += f"Get Data: \n{self.GetDataConfig.__str__()}"
        elif self.mode == "process-data":
            s += f"Process Data: \n{self.ProcessDataConfig.__str__()}"
        elif self.mode == "all":
            s += f"Get Data: \n{self.GetDataConfig.__str__()}"
            s += "\n*************************\n\n"
            s += f"Process Data: \n{self.ProcessDataConfig.__str__()}"
        else:
            raise ValueError("Not a valid mode!")
        s += "\n*************************\n\n"
        return s


def _countries_to_modules(s):
        if s == "all":
            return modules_name
        elif s == "incremental":
            return modules_name_incremental
        elif s == "batch":
            return modules_name_batch
        # Comma separated string to list of strings
        countries = [ss.strip().replace(" ", "_").lower() for ss in s.split(",")]
        # Verify validity of countries
        countries_wrong = [c for c in countries if c not in country_to_module]
        if countries_wrong:
            print(f"Invalid countries: {countries_wrong}. Valid countries are: {list(country_to_module.keys())}")
            raise ValueError("Invalid country")
        # Get module equivalent names
        modules = [country_to_module[country] for country in countries]
        return modules
