function usage {
  echo "Usage: $0 [OPTION]..."
  echo "Run tests"
  echo ""
  echo "  -n, --no-virtual-env        Don't use virtualenv.  Run tests in local environment"
  echo "  -r, --rebuild               Force a clean re-build of the virtual environment. Useful when dependencies have been added."
  echo "  -u, --update                Update the virtual environment with any newer package versions"
  echo "  -p, --pep8                  Just run PEP8 and HACKING compliance check"
  echo "  -P, --no-pep8               Don't run static code checks"
  echo "  -c, --coverage              Generate coverage report"
  echo "  -h, --help                  Print this usage message"
  echo "  --virtual-env-path <path>   Location of the virtualenv directory"
  echo "                               Default: \$(pwd)"
  echo "  --virtual-env-name <name>   Name of the virtualenv directory"
  echo "                               Default: .venv"
  echo ""
  echo "Note: with no options specified, the script will try to run the tests in a virtual environment,"
  echo "      If no virtualenv is found, the script will ask if you would like to create one.  If you "
  echo "      prefer to run tests NOT in a virtual environment, simply pass the -N option."
  exit
}

function process_options {
  i=1
  while [ $i -le $# ]; do
    case "${!i}" in
      -h|--help) usage;;
      -n|--no-virtual-env) no_venv=1;;
      -r|--rebuild) rebuild=1;;
      -u|--update) update=1;;
      -p|--pep8) just_pep8=1;;
      -P|--no-pep8) no_pep8=1;;
      -c|--coverage) coverage=1;;
      --virtual-env-path)
        (( i++ ))
        venv_path=${!i}
        ;;
      --virtual-env-name)
        (( i++ ))
        venv_dir=${!i}
        ;;
    esac
    (( i++ ))
  done
}

function wrapper {
    (
        source ${venv}/bin/activate && $@
    )
}

venv_path=${venv_path:-$(pwd)} 
venv_dir=${venv_name:-.venv}
venv=${venv_path}/${venv_dir}
with_venv=wrapper
no_venv=0
rebuild=0
just_pep8=0
no_pep8=0
coverage=0
update=0

process_options $@


function run_tests {
    if [ $coverage -eq 1 ]; then
        TEST_ARGS="--with-coverage --cover-package=simulator"
    fi

    find . -name *pyc -exec rm {} +

    set +e
    echo
    echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
    echo -e "\t Running unit tests"
    ${wrapper} nosetests -v $TEST_ARGS
    RESULT=$?
    echo "<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<"
    echo
    set -e

    if [ $coverage -eq 1 ]; then
        echo
        echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
        echo "Generating coverage report in cover/"
        ${wrapper} coverage combine
        ${wrapper} coverage html --include='simulator/*' -d cover -i
        echo "<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<"
    fi
    return $RESULT
}

function run_pep8 {
    echo
    echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
    echo -e "\t Running PEP8"
    ${wrapper} pep8 simulator && echo OK
    RETURN=$?
    echo "<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<"
    echo
    return $RETURN
}

function install_venv {
    virtualenv ${venv}
    wrapper pip install -r requirements.txt
    wrapper pip install -r test-requirements.txt
}


if [ $no_venv -eq 0 ]; then
    # Remove the virtual environment if --rebuild used
    if [ $rebuild -eq 1 ]; then
        echo "Cleaning virtualenv..."
        rm -rf ${venv}
    fi
    if [ $update -eq 1 ]; then
        echo "Updating virtualenv..."
        install_venv
    fi
    if [ -e ${venv} ]; then
        wrapper="${with_venv}"
    else
        read use_ve
        if [ "x$use_ve" = "xY" -o "x$use_ve" = "x" -o "x$use_ve" = "xy" ]; then
            # Install the virtualenv and run the test suite in it
            install_venv
            wrapper=${with_venv}
        fi
    fi
fi


# Delete old coverage data from previous runs
if [ $coverage -eq 1 ]; then
    ${wrapper} coverage erase
fi

if [ $just_pep8 -eq 1 ]; then
    run_pep8
    exit
fi

run_tests

# NOTE(sirp): we only want to run pep8 when we're running the full-test suite,
# not when we're running tests individually. To handle this, we need to
# distinguish between options (testropts), which begin with a '-', and
# arguments (testrargs).
if [ $no_pep8 -eq 0 ]; then
    run_pep8
fi

