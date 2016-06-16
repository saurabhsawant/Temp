from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
import utils

# Create your views here.


class ReprocessCaller(APIView):
    """
    Redirects POST with action Labels from boy to the Luigi tasks in order to reprocess data cubes.
    """
    def post(self, request, format=None):
        try:
            utils.validate_request_data(request)
        except ValueError as e:
            response_data = dict()
            # standard format to be followed for custom exceptions
            response_data['message'] = "Exception : " + e.__class__.__name__ + " :- " + e.__str__()
            utils.LOGGER.error("Validation during validation: " + response_data['message'])
            return Response(response_data, status=status.HTTP_400_BAD_REQUEST)
        finally:
            pass

        # process the request as per action value passed in body
        return utils.process_request(request)

