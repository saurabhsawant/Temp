from django.test import SimpleTestCase, Client
from django.test.client import RequestFactory
from rest_framework.test import APIRequestFactory
from rest_framework import status
import utils
from rest_framework.response import Response
import mock
import os

def mock_for_validate_request_data(request):
    pass


def mock_for_process_request(request_data):
    return Response("I am mock do not trust my message!", status=status.HTTP_200_OK)


def format_request_body(provider, start_time, end_time, action, environment, cube, url_dimension):
    data = dict()
    data['provider'] = provider
    data['start_time'] = start_time
    data['end_time'] = end_time
    data['action'] = action
    data['environment'] = environment
    data['cube'] = cube
    data['url_dimension'] = url_dimension
    return data


class SimpleTestCaseClass(SimpleTestCase):
    pass # Need to fix test cases as per new changes

    def setUp(self):
        # Every test needs a client.
        self.client = Client()

    def test_post_invalid_json_post(self):
        # Expected response HTTP_400_BAD_REQUEST
        response = self.client.post(path='/datacube_accuracy_api/',
                                    data='{incorrect-json-$#%$#%#$%}',
                                    content_type='application/json')
        self.assertEquals(response.status_code, status.HTTP_400_BAD_REQUEST)

    @mock.patch('datacube_accuracy_api.utils.validate_request_data', side_effect=mock_for_validate_request_data)
    @mock.patch('datacube_accuracy_api.utils.process_request', side_effect=mock_for_process_request)
    def test_post_with_valid_json_check(self, fun1, fun2):
        # Expected response HTTP_200_OK
        response = self.client.post(path='/datacube_accuracy_api/',
                                    data='{"start_time":"2016-01-01T00:00",'
                                         '"end_time":"2016-01-02T00:00"}',
                                    content_type='application/json')
        self.assertEquals(response.status_code, status.HTTP_200_OK)

    @mock.patch('datacube_accuracy_api.utils.validate_request_data', side_effect=mock_for_validate_request_data)
    @mock.patch('datacube_accuracy_api.utils.process_request', side_effect=mock_for_process_request)
    def test_post_with_valid_json_reprocess(self, fun1, fun2):
        # Expected response HTTP_201_CREATED
        response = self.client.post(path='/datacube_accuracy_api/',
                                    data='{"action":"generate"}',
                                    content_type='application/json')

        self.assertEquals(response.status_code, status.HTTP_200_OK)

    def test_get_methods(self):
        # Expected response HTTP_405_METHOD_NOT_ALLOWED
        response = self.client.get(path='/datacube_accuracy_api/')
        self.assertEquals(response.status_code, status.HTTP_405_METHOD_NOT_ALLOWED)

    def test_put_methods(self):
        # Expected response HTTP_405_METHOD_NOT_ALLOWED
        response = self.client.put(path='/datacube_accuracy_api/')
        self.assertEquals(response.status_code, status.HTTP_405_METHOD_NOT_ALLOWED)

    def test_delete_methods(self):
        # Expected response HTTP_405_METHOD_NOT_ALLOWED
        response = self.client.delete(path='/datacube_accuracy_api/')
        self.assertEquals(response.status_code, status.HTTP_405_METHOD_NOT_ALLOWED)

    def test_head_methods(self):
        # Expected response HTTP_405_METHOD_NOT_ALLOWED
        response = self.client.head(path='/datacube_accuracy_api/')
        self.assertEquals(response.status_code, status.HTTP_405_METHOD_NOT_ALLOWED)

    def test_post_with_valid_json_delete(self):
        # Expected response status.HTTP_405_METHOD_NOT_ALLOWED
        response = self.client.post(path='/datacube_accuracy_api/',
                                    data='{"start_time":"2016-01-01T00:00",'
                                         '"end_time":"2016-01-02T00:00",'
                                         '"action":"delete",'
                                         '"cube":"min15",'
                                         '"environment":"nst",'
                                         '"url_dimension":"yes"}',
                                    content_type='application/json')
        self.assertEquals(response.status_code, status.HTTP_405_METHOD_NOT_ALLOWED)

    def test_validate_request_data_incorrect_start_date(self):
        request = RequestFactory().post(path="/datacube_accuracy_api/",
                                        data=format_request_body(provider="blah-blah",
                                                                 start_time="invalid",  # invalid start format
                                                                 end_time="2016-01-02T00:00",
                                                                 action=utils.ACTION_CHECK,
                                                                 environment=utils.ENVIRONMENT_NEXT_STAGING,
                                                                 cube=utils.CUBE_DAILY,
                                                                 url_dimension=utils.URL_DIMENSION_YES))
        # overriding data attribute as mismatch between WSGIRequest and Django request in APIViews
        request.data = request.POST
        """
        # self.assertRaises(ValueError, utils.validate_request_data(request))
        try:
            utils.validate_request_data(request)
            self.assertFalse(True)
        except ValueError:
            self.assertTrue(True)
        """
        with self.assertRaisesMessage(ValueError,
                                      "Incorrect start_time format in request body. Please provide \'%Y-%m-%dT%H:%M\'"):
            utils.validate_request_data(request)

    def test_validate_request_data_incorrect_start_minute(self):
        request = RequestFactory().post(path="/datacube_accuracy_api/",
                                        data=format_request_body(provider="blah-blah",
                                                                 start_time="2016-01-01T00:11",  # invalid minute format
                                                                 end_time="2016-01-02T00:00",
                                                                 action=utils.ACTION_CHECK,
                                                                 environment=utils.ENVIRONMENT_NEXT_STAGING,
                                                                 cube=utils.CUBE_DAILY,
                                                                 url_dimension=utils.URL_DIMENSION_YES))
        # overriding data attribute as mismatch between WSGIRequest and Django request in APIViews
        request.data = request.POST
        with self.assertRaisesMessage(ValueError,
                                      "Incorrect start_time. Minute should start from 00 or 15 or 30 or 45"):
            utils.validate_request_data(request)

    def test_validate_request_data_incorrect_end_date(self):
        request = RequestFactory().post(path="/datacube_accuracy_api/",
                                        data=format_request_body(provider="blah-blah",
                                                                 start_time="2016-01-01T00:00",
                                                                 end_time="33/33/33",  # invalid date format
                                                                 action=utils.ACTION_CHECK,
                                                                 environment=utils.ENVIRONMENT_NEXT_STAGING,
                                                                 cube=utils.CUBE_DAILY,
                                                                 url_dimension=utils.URL_DIMENSION_YES))
        # overriding data attribute as mismatch between WSGIRequest and Django request in APIViews
        request.data = request.POST
        with self.assertRaisesMessage(ValueError,
                                      "Incorrect end_time format in request body. Please provide \'%Y-%m-%dT%H:%M\'"):
            utils.validate_request_data(request)

    def test_validate_request_data_incorrect_end_minute(self):
        request = RequestFactory().post(path="/datacube_accuracy_api/",
                                        data=format_request_body(provider="blah-blah",
                                                                 start_time="2016-01-01T00:00",
                                                                 end_time="2016-01-02T00:11",  # invalid minute format
                                                                 action=utils.ACTION_CHECK,
                                                                 environment=utils.ENVIRONMENT_NEXT_STAGING,
                                                                 cube=utils.CUBE_DAILY,
                                                                 url_dimension=utils.URL_DIMENSION_YES))
        # overriding data attribute as mismatch between WSGIRequest and Django request in APIViews
        request.data = request.POST
        with self.assertRaisesMessage(ValueError,
                                      "Incorrect end_time. Minute should start from 00 or 15 or 30 or 45"):
            utils.validate_request_data(request)

    def test_validate_request_data_incorrect_date_range(self):
        request = RequestFactory().post(path="/datacube_accuracy_api/",
                                        data=format_request_body(provider="blah-blah",
                                                                 start_time="2016-01-02T00:00",
                                                                 end_time="2016-01-01T00:00",
                                                                 action=utils.ACTION_CHECK,
                                                                 environment=utils.ENVIRONMENT_NEXT_STAGING,
                                                                 cube=utils.CUBE_DAILY,
                                                                 url_dimension=utils.URL_DIMENSION_YES))
        # overriding data attribute as mismatch between WSGIRequest and Django request in APIViews
        request.data = request.POST
        with self.assertRaisesMessage(ValueError,
                                      "start_time:2016-01-02T00:00 is greater than end_time:2016-01-01T00:00"
                                      " in request body"):
            utils.validate_request_data(request)

    def test_validate_request_data_incorrect_cube(self):
        request = RequestFactory().post(path="/datacube_accuracy_api/",
                                        data=format_request_body(provider="blah-blah",
                                                                 start_time="2016-01-01T00:00",
                                                                 end_time="2016-01-02T00:00",
                                                                 action=utils.ACTION_CHECK,
                                                                 environment=utils.ENVIRONMENT_NEXT_STAGING,
                                                                 cube="some incorrect value",
                                                                 url_dimension=utils.URL_DIMENSION_YES))
        # overriding data attribute as mismatch between WSGIRequest and Django request in APIViews
        request.data = request.POST
        with self.assertRaisesMessage(ValueError,
                                      "Incorrect 'cube' : " + "some incorrect value" + " value. Should be either " \
                                      + utils.CUBE_MIN_15 + "/" + utils.CUBE_DAILY + "/" + utils.CUBE_WEEKLY):
            utils.validate_request_data(request)


    def test_validate_request_data(self):
        request = RequestFactory().post(path="/datacube_accuracy_api/",
                                        data=format_request_body(provider="blah-blah",
                                                                 start_time="2016-01-01T00:00",
                                                                 end_time="2016-01-02T00:00",
                                                                 action=utils.ACTION_CHECK,
                                                                 environment=utils.ENVIRONMENT_NEXT_STAGING,
                                                                 cube=utils.CUBE_DAILY,
                                                                 url_dimension=utils.URL_DIMENSION_YES))
        # overriding data attribute as mismatch between WSGIRequest and Django request in APIViews
        request.data = request.POST
        try:
            utils.validate_request_data(request)
        except ValueError:
            self.assertTrue(False)

    def test_process_request_invalid_action(self):
        response = self.client.post(path='/datacube_accuracy_api/',
                                    data=format_request_body(provider="blah-blah",
                                                             start_time="2016-01-01T00:00",
                                                             end_time="2016-01-02T00:00",
                                                             action="Some Invalid Action",
                                                             environment=utils.ENVIRONMENT_NEXT_STAGING,
                                                             cube=utils.CUBE_DAILY,
                                                             url_dimension=utils.URL_DIMENSION_YES))
        self.assertEquals(response.status_code, status.HTTP_405_METHOD_NOT_ALLOWED)

    def test_read_file_with_expected_results_test(self):
        dir = os.path.dirname(__file__)
        filename = os.path.join(dir, './resources/test_file.csv')
        print filename
        list_from_file = utils.read_file(filename)
        list_expected = dict()
        list_expected['2016-01-01 03:00:00'] = 'not_completed'
        list_expected['2016-01-01 02:45:00'] = 'not_completed'
        list_expected['2016-01-01 08:15:00'] = 'not_completed'
        list_expected['2016-01-01 04:00:00'] = 'completed'

        self.assertEquals(len(set(list_from_file.items()) & set(list_expected.items())), len(list_from_file))


