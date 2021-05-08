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
        cfg = config.ProcessDataConfig
        main_process_data(
            paths,
            cfg.google_credentials,
            cfg.google_spreadsheet_vax_id,
            cfg.skip_complete,
            cfg.skip_monotonic_check,
        )
    elif config.mode == "all":
        cfg = config.GetDataConfig
        main_get_data(
            paths,
            cfg.parallel,
            cfg.njobs,
            cfg.countries,
            cfg.greece_api_token,
        )
        cfg = config.ProcessDataConfig
        main_process_data(
            paths,
            cfg.google_credentials,
            cfg.google_spreadsheet_vax_id,
            cfg.skip_complete,
            cfg.skip_monotonic_check,
        )


if __name__ == "__main__":
    main()
