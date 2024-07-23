# Dependencies

Python verion `3.10` with the following packages:

- Biopython, version `1.83`
- Psycopg2, version `2.9.9`
- Requests, version `2.31`

## Create Virtual Enviroment

```shell
python3.10 -m venv /path/to/my/venv
```

The `source` command is used to activate the virtual enviroment.

```shell
source /path/to/my/venv/bin/activate
```

Once activated, you can deactivate the enviorment using `deactivate`.

See the [Python Documentation](<https://docs.python.org/3/library/venv.html>) for
troubleshooting and more detailed information about how to create and manage virtual environments.

## Installing Required Dependencies

With the virtual enviroment activated, you can install the required dependencies with:

```shell
pip install -r config/requirements.txt
```
