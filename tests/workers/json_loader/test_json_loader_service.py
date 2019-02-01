__author__ = "Ivo Willemsen (IWIO)"
__date__ = "2018-11-26"

import unittest
from mock import patch
import os
import worker
import json
from shared import load_file

ROOT = os.path.dirname(os.path.realpath(__file__))
CONFIG_DIR = "../config"

PACKAGE = "workers.json_loader.json_loader_service"


@patch(PACKAGE + ".JsonDao")
class TestJsonLoaderService(unittest.TestCase):

    mock = None

    def test_process_json(self, mock_json_dao):
        TestJsonLoaderService.setMock(mock_json_dao)
        with patch(PACKAGE + ".JsonLoaderService.load_data",
                   return_value=json.load(load_file(ROOT + "/" + CONFIG_DIR + "/" + "test_json.json"))):
            result = [data for data in worker.process_json(None, None, None, None, None)]
            self.assertEquals(len(result), 4)
            self.assertEquals(TestJsonLoaderService.mock.return_value.add_annotation.call_count, 2)
            self.assertEquals(TestJsonLoaderService.mock.return_value.add_limit.call_count, 2)

    def test_process_nothing(self, mock_json_dao):
        mock_json_dao.return_value.reset_mock()
        with patch(PACKAGE + ".JsonLoaderService.load_data",
                   return_value=json.load(load_file(ROOT + "/" + CONFIG_DIR + "/" + "test_json_nothing.json"))):
            result = [data for data in worker.process_json(None, None, None, None, None)]
            self.assertEquals(len(result), 0)
            self.assertEquals(mock_json_dao.return_value.add_annotation.call_count, 0)
            self.assertEquals(mock_json_dao.return_value.add_limit.call_count, 0)

    def test_process_empty(self, mock_json_dao):
        TestJsonLoaderService.setMock(mock_json_dao)
        with patch(PACKAGE + ".JsonLoaderService.load_data",
                   return_value=json.load(load_file(ROOT + "/" + CONFIG_DIR + "/" + "test_json_empty_elements.json"))):
            result = [data for data in worker.process_json(None, None, None, None, None)]
            self.assertEquals(len(result), 0)
            self.assertEquals(mock_json_dao.return_value.add_annotation.call_count, 0)
            self.assertEquals(mock_json_dao.return_value.add_limit.call_count, 0)

    def test_process_json_no_annotations(self, mock_json_dao):
        TestJsonLoaderService.setMock(mock_json_dao)
        with patch(PACKAGE + ".JsonLoaderService.load_data",
                   return_value=json.load(load_file(ROOT + "/" + CONFIG_DIR + "/" + "test_json_no_annotations.json"))):
            worker.process_json(None, None, None, None, None)
            self.assertEquals(mock_json_dao.return_value.add_annotation.call_count, 0)

    def test_process_json_no_limits(self, mock_json_dao):
        TestJsonLoaderService.setMock(mock_json_dao)
        with patch(PACKAGE + ".JsonLoaderService.load_data",
                   return_value=json.load(load_file(ROOT + "/" + CONFIG_DIR + "/" + "test_json_no_limits.json"))):
            worker.process_json(None, None, None, None, None)
            self.assertEquals(mock_json_dao.return_value.add_limits.call_count, 0)

    @classmethod
    def setMock(cls, mock):
        if not TestJsonLoaderService.mock:
            TestJsonLoaderService.mock = mock
        TestJsonLoaderService.mock.reset_mock()
