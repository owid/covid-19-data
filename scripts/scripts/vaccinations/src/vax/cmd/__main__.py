import argparse

from vax.cmd._config import get_config
from vax.cmd import main_get_data, main_process_data
from vax.utils.paths import Paths


def main():
    config = get_config()
    paths = Paths(config.project_dir)

    if config.display:
        print(config)
    
    if config.mode == "get-data":
        cfg = config.GetDataConfig
        main_get_data(
            paths,
            cfg.parallel,
            cfg.njobs,
            cfg.countries,
            cfg.greece_api_token,
        )
    elif config.mode == "process-data":
        main_process_data()
    elif config.mode == "all":
        main_get_data(
            config.GetDataConfig.output_dir,
            config.GetDataConfig.parallel,
            config.GetDataConfig.njobs,
            config.GetDataConfig.countries
        )
        main_process_data()


if __name__ == "__main__":
    main()
