import calendar
from datetime import time
from rest_framework.response import Response
from rest_framework import status
import subprocess
import logging
import logging.config
from datetime import datetime
import json
import os

# Constants
ACTION_CHECK = "check"
ACTION_GENERATE = "generate"
ACTION_DELETE = "delete"

ENVIRONMENT_STAGING = "stg"
ENVIRONMENT_NEXT_STAGING = "nst"
ENVIRONMENT_PROD = "prod"

CUBE_MIN_15 = "min15"
CUBE_DAILY = "daily"
CUBE_WEEKLY = "weekly"

URL_DIMENSION_YES = "yes"
URL_DIMENSION_NO = "no"

DATE_INPUT_FORMAT = '%Y-%m-%dT%H:%M'
DATE_LUIGI_FORMAT = '%Y-%m-%dT%H%M'

# config with the root?
LOGGER_FILE_PATH = '/Users/ssawant/repos/wario/reprocess_api/reprocess_api/resources/api_logging.conf'


def get_logger():
    """ Returns the Logger instance for API logging"""
    logging.config.fileConfig(LOGGER_FILE_PATH)
    return logging.getLogger('api_logger')


# Logger instance
LOGGER = get_logger()


# util methods
# throw an exception with the message instead of boolean and message
def validate_request_data(request):
    try:
        start_time = datetime.strptime(request.data['start_time'], DATE_INPUT_FORMAT)
        request.data['start_time'] = start_time.strftime(DATE_LUIGI_FORMAT)
    except ValueError:
        raise ValueError("Incorrect start_time format in request body. Please provide \'%Y-%m-%dT%H:%M\'")

    if start_time.minute != 00 and start_time.minute != 15 and start_time.minute != 30 and start_time.minute != 45:
        raise ValueError("Incorrect start_time. Minute should start from 00 or 15 or 30 or 45")

    try:
        end_time = datetime.strptime(request.data['end_time'], DATE_INPUT_FORMAT)
        request.data['end_time'] = end_time.strftime(DATE_LUIGI_FORMAT)
    except ValueError:
        raise ValueError("Incorrect end_time format in request body. Please provide \'%Y-%m-%dT%H:%M\'")

    if end_time.minute != 00 and end_time.minute != 15 and end_time.minute != 30 and end_time.minute != 45:
        raise ValueError("Incorrect end_time. Minute should start from 00 or 15 or 30 or 45")

    if start_time > end_time:
        print start_time
        print end_time
        raise ValueError("start_time:{} is greater than end_time:{} in request body".
                         format(start_time.strftime(DATE_INPUT_FORMAT), end_time.strftime(DATE_INPUT_FORMAT)))

    if CUBE_MIN_15 != request.data['cube'] and CUBE_DAILY != request.data['cube'] \
            and CUBE_WEEKLY != request.data['cube']:
        raise ValueError("Incorrect 'cube' : " + request.data[
            'cube'] + " value. Should be either " + CUBE_MIN_15 + "/" + CUBE_DAILY + "/" + CUBE_WEEKLY)

    # Validate minimum data keys for min15


# process request
def process_request(request):
    """
    Returns the response generated by appropriate function for action

    Keyword arguments:
    request -- the request object by an on call UI
    """
    # processing request as per action value
    LOGGER.debug("Processing action : %s.", request.data["action"])

    if request.data["action"] == ACTION_CHECK:
        return response_for_check(request.data)
    elif request.data["action"] == ACTION_GENERATE:
        return response_for_generate(request.data)
        # return Response(response_for_check(request), status=status.HTTP_201_CREATED)
    elif request.data["action"] == ACTION_DELETE:
        response = {"message": "Action  " + ACTION_DELETE + "is not supported yet"}
        return Response(response, status=status.HTTP_405_METHOD_NOT_ALLOWED)
    else:
        response = {"message": "Action  " + request.data["action"] + " is not supported yet"}
        return Response(response, status=status.HTTP_405_METHOD_NOT_ALLOWED)


def response_for_check(request_data):
    """
    Returns the response generated by appropriate function for action check
    Which checks what data cubes are completed or not completed in the given range

    Keyword arguments:
    request_data -- the request object body containing json data by an on call UI
    """
    # Opening a Shell for calling Luigi Tasks rather than a hook as luigi needs Target to check for
    # execution history of task ran before
    # Avoiding adding additional Luigi.Task. But this will take a hit for multi user race condition
    # opening a synchronous call to a shell command
    command = list()
    command.append("python")
    command.append("-m")
    command.append("luigi")
    command.append("--module")
    command.append("wario.check_cube_targets")
    command.append("CmvCheckCubeTargets")
    command.append("--start-time={}".format(request_data['start_time']))
    command.append("--end-time={}".format(request_data['end_time']))
    command.append("--task={}".format(request_data['cube']))

    LOGGER.debug(command)

    # response jason data mainly consists of message and response
    # in case of errors, only the message will be populated
    response_data = dict()
    try:
        LOGGER.info("subprocess.check_call  for command : %s", command)
        # Synchronous call, not more than couple of seconds
        subprocess.check_call(command)

        # o/p file created by the CmvCheckCubeTargets Task
        file_to_read = "/var/log/luigi/luigi_targets/checkcubes/{}_{}_{}.csv".format(request_data['start_time'],
                                                                                     request_data['end_time'],
                                                                                     request_data['cube'])
        response_data['response'] = read_file(file_to_read)
    # TODO handle specific exceptions in case diffrent logging is required
    except Exception as e:
        # standard format to be followed for custom exceptions
        response_data['message'] = "Exception : " + e.__class__.__name__ + " :- " + e.__str__()

        LOGGER.error(response_data['message'])
        return Response(response_data, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

    # Success, the results must be present in response_data['response'] key
    response_data['message'] = "Sending results for start_time: {start_time}, end_time: {end_time} and cube: {cube}". \
        format(start_time=request_data['start_time'],
               end_time=request_data['end_time'],
               cube=request_data['cube'])
    LOGGER.info(response_data['message'])
    return Response(response_data, status=status.HTTP_200_OK)


# TODO have a common file write read functions for luigi tasks and API, so validations are in place
# parsing file in format "date1 time1=status1,date2 time2=status2," to {date1 time1:status1, date2 time2:status2}
def read_file(file_to_read):
    """
    Reads the csv file which is the output / target of the luigi job for data cube checking
    Returns the dictionary with key as data cube time and value as status (completed or not_completed)

    Keyword arguments:
    file_to_read -- is the absolute path of a file to read / relative as compared to current directory placement
    """
    LOGGER.debug("Reading File  : %s", file_to_read)
    # TODO json format to replace for the file in waro as txt and  csv
    with open(file_to_read, 'r') as f:
        data = f.read()
    # removing the last empty split string
    results = data.split(",")[:-1]  # delimiter (Expecting less entries so the read string wont go out of memory)

    result_dict = dict()
    for result in results:
        key_value = result.split("=")
        result_dict[key_value[0]] = key_value[1]

    return result_dict


def response_for_generate(request_data):
    """
    Returns the response generated by appropriate function for action generate data cube
    This one will initiate the luigi task CmvReprocessAPIHook which will in tern schedule respective jobs/tasks for
    data cube reprocessing

    Keyword arguments:
    request_data -- the request object body containing json data by an on call UI
    """
    # Opening a Shell for calling Luigi Tasks rather than a hook as luigi needs Target to check for
    # execution history of task ran before
    # Avoiding adding additional Luigi.Task. But this will take a hit for multi user race condition
    # opening a synchronous call to a shell command
    command = list()
    command.append("python")
    command.append("-m")
    command.append("luigi")
    command.append("--module")
    command.append("wario.reprocess_datacube")
    command.append("CmvReprocessAPIHook")
    command.append("--param={}".format(json.dumps(request_data)))

    LOGGER.debug(command)

    # response jason data mainly consists of message and response
    # in case of errors, only the message will be populated
    response_data = dict()
    try:
        LOGGER.info("subprocess.check_call  for command : %s", command)
        # Synchronous call, not more than couple of seconds
        #my_env = os.environ.copy()
        #my_env["HADOOP_HOME"] = "/Users/ssawant/repos/vendor/hadoop_distros/current"
        #my_env["HADOOP_CONF_DIR"] = "/Users/ssawant/repos/vendor/hadoop_distros/current/conf-cdh5-staging"
        subprocess.check_call(command) #, env=my_env)
    # TODO handle specific exceptions in case diffrent logging is required
    except Exception as e:
        # standard format to be followed for custom exceptions
        response_data['message'] = "Exception : " + e.__class__.__name__ + " :- " + e.__str__()

        LOGGER.error(response_data['message'])
        return Response(response_data, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

    # Success, the results must be present in response_data['response'] key
    response_data['message'] = "CmvReprocessAPIHook Job scheduled."
    LOGGER.info(response_data['message'])
    return Response(response_data, status=status.HTTP_201_CREATED)
