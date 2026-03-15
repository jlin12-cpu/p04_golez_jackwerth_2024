"""
Load project configuration from command line arguments, environment
variables, and .env files.

This module provides a single configuration interface for the project.
It is designed to support reproducible analytical pipelines by keeping
project-wide settings (such as data paths, output paths, WRDS username,
and analysis date range) in one place.

Order of precedence
-------------------
Configuration values are resolved in the following order:

1. Command line arguments
2. Environment variables / .env file
3. Defaults defined in this file
4. Local default passed directly into config(...)

This makes it easy to:
- keep sensible project defaults in settings.py,
- override them in a local .env file,
- and temporarily override them again from the command line.

Example
-------
Create a file called `myexample.py` with the following content:

    from settings import config
    DATA_DIR = config("DATA_DIR")

    print(f"Using DATA_DIR: {DATA_DIR}")

and run either:

    python myexample.py --DATA_DIR=/path/to/data

or:

    export DATA_DIR=/path/to/other
    python myexample.py

If `settings.py` is run directly, it will create the standard project
directories if they do not already exist.
"""

import sys
from datetime import datetime
from pathlib import Path
from platform import system

from decouple import config as _config


def find_all_caps_cli_vars(argv=sys.argv):
    """Find command line variables written in all caps.

    This function detects long-form command line options such as:

        --DATA_DIR=/path/to/data
        --WRDS_USERNAME=myusername
        --START_DATE=1996-01-01

    It also supports the form:

        --DATA_DIR /path/to/data

    Parameters
    ----------
    argv : list
        Command line argument list, usually sys.argv.

    Returns
    -------
    dict
        Dictionary mapping variable names to their command line values.
    """
    result = {}
    i = 0
    while i < len(argv):
        arg = argv[i]

        # Handle --VAR=value
        if arg.startswith("--") and "=" in arg and arg[2:].split("=")[0].isupper():
            var_name, value = arg[2:].split("=", 1)
            result[var_name] = value

        # Handle --VAR value
        elif arg.startswith("--") and arg[2:].isupper() and i + 1 < len(argv):
            var_name = arg[2:]
            value = argv[i + 1]
            if not value.startswith("--"):
                result[var_name] = value
                i += 1

        i += 1

    return result


cli_vars = find_all_caps_cli_vars()

# =============================================================================
# Defaults
# =============================================================================

defaults = {}


# Absolute path to project root
if "BASE_DIR" in cli_vars:
    defaults["BASE_DIR"] = Path(cli_vars["BASE_DIR"])
else:
    defaults["BASE_DIR"] = Path(__file__).absolute().parent.parent


def get_os():
    """Return simplified OS type string."""
    os_name = system()
    if os_name == "Windows":
        return "windows"
    elif os_name in {"Darwin", "Linux"}:
        return "nix"
    else:
        return "unknown"


if "OS_TYPE" in cli_vars:
    defaults["OS_TYPE"] = cli_vars["OS_TYPE"]
else:
    defaults["OS_TYPE"] = get_os()


def get_stata_exe():
    """Return the default Stata executable name for the current OS."""
    if defaults["OS_TYPE"] == "windows":
        return "StataMP-64.exe"
    elif defaults["OS_TYPE"] == "nix":
        return "stata-mp"
    else:
        raise ValueError("Unknown OS type")


if "STATA_EXE" in cli_vars:
    defaults["STATA_EXE"] = cli_vars["STATA_EXE"]
else:
    defaults["STATA_EXE"] = get_stata_exe()


# Default analysis date range for this project
defaults["START_DATE"] = datetime.strptime("1996-01-01", "%Y-%m-%d")
defaults["END_DATE"] = datetime.strptime("2025-12-31", "%Y-%m-%d")


def if_relative_make_abs(path):
    """Convert a relative path to an absolute path rooted at BASE_DIR.

    Parameters
    ----------
    path : str or Path
        Input path.

    Returns
    -------
    Path
        Absolute resolved path.
    """
    path = Path(path)
    if path.is_absolute():
        abs_path = path.resolve()
    else:
        abs_path = (defaults["BASE_DIR"] / path).resolve()
    return abs_path


defaults = {
    "DATA_DIR": if_relative_make_abs(Path("_data")),
    "MANUAL_DATA_DIR": if_relative_make_abs(Path("data_manual")),
    "OUTPUT_DIR": if_relative_make_abs(Path("output")),
    **defaults,
}


def config(
    var_name,
    default=None,
    cast=None,
    settings_py_defaults=defaults,
    cli_vars=cli_vars,
    convert_dir_vars_to_abs_path=True,
):
    """Read a configuration value using project precedence rules.

    Parameters
    ----------
    var_name : str
        Name of the configuration variable.
    default : optional
        Local fallback default if the variable is not found elsewhere.
    cast : callable, optional
        Function used to cast the value after loading.
    settings_py_defaults : dict
        Project defaults defined in this file.
    cli_vars : dict
        Command line variables extracted from sys.argv.
    convert_dir_vars_to_abs_path : bool
        If True, variables containing 'DIR' in the name are converted to
        absolute Path objects.

    Returns
    -------
    object
        Configuration value.

    Raises
    ------
    ValueError
        If the variable cannot be found anywhere and no default is supplied.
    """

    # 1. Command line arguments (highest priority)
    if var_name in cli_vars and cli_vars[var_name] is not None:
        value = cli_vars[var_name]
        if cast is not None:
            value = cast(value)
        if "DIR" in var_name and convert_dir_vars_to_abs_path:
            value = if_relative_make_abs(Path(value))
        return value

    # 2. Environment variables / .env
    env_sentinel = object()
    env_value = _config(var_name, default=env_sentinel)
    if env_value is not env_sentinel:
        if cast is not None:
            env_value = cast(env_value)
        if "DIR" in var_name and convert_dir_vars_to_abs_path:
            env_value = if_relative_make_abs(Path(env_value))
        return env_value

    # 3. settings.py defaults
    if var_name in settings_py_defaults:
        default_value = settings_py_defaults[var_name]
        if cast is not None:
            default_value = cast(default_value)
        return default_value

    # 4. Local inline default
    try:
        return _config(var_name, default=default, cast=cast)
    except Exception as e:
        raise ValueError(
            f"Configuration variable '{var_name}' is not defined.\n"
            f"Set it in one of these ways:\n"
            f"  1. Command line: --{var_name}=value\n"
            f"  2. Environment variable: export {var_name}=value\n"
            f"  3. .env file: {var_name}=value\n"
            f"Original error: {e}"
        ) from e


def create_directories():
    """Create the standard project directories if they do not exist."""
    config("DATA_DIR").mkdir(parents=True, exist_ok=True)
    config("OUTPUT_DIR").mkdir(parents=True, exist_ok=True)
    config("MANUAL_DATA_DIR").mkdir(parents=True, exist_ok=True)


if __name__ == "__main__":
    create_directories()
    print("Created/verified project directories:")
    print(f"  DATA_DIR       = {config('DATA_DIR')}")
    print(f"  MANUAL_DATA_DIR= {config('MANUAL_DATA_DIR')}")
    print(f"  OUTPUT_DIR     = {config('OUTPUT_DIR')}")
    print(f"  START_DATE     = {config('START_DATE')}")
    print(f"  END_DATE       = {config('END_DATE')}")