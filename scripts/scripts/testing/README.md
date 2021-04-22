# Testing update automation



## Run automated pipelines

```bash
$ python3 run_python_scripts.py [option]
$ Rscript run_r_scripts.R [option]
```

Note: Accepted values for `option` are: "quick" and "update". VM runs this everyday with "quick option". Manual
execution with mode "update" is required ~twice a week (e.g. tuesday and friday).

## Generate dataset

Run `generate_dataset.R`. Usage of RStudio is recommended.

## Update grapher & co data

Run

```bash
$ python ../megafile.py
```

Note: May want to use [`test_update.sh.template`](test_update.sh.template) as a reference.