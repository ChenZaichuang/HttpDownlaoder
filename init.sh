SCRIPT_PATH="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
pip3 install -r ${SCRIPT_PATH}/requirements.txt
find ${SCRIPT_PATH} -type d -empty -delete