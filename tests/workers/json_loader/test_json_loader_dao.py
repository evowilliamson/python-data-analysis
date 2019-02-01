__author__ = "Ivo Willemsen (IWIO)"
__date__ = "2018-11-26"

import unittest
from mock import patch
import os
from workers.json_loader.json_loader_dao import JsonDao

ROOT = os.path.dirname(os.path.realpath(__file__))
CONFIG_DIR = "../config"

PACKAGE = "workers.json_loader.json_loader_dao"


@patch(PACKAGE + ".DbClientFactory")
@patch(PACKAGE + ".DbClientConfig")
@patch(PACKAGE + ".Base")
class TestJsonLoaderDao(unittest.TestCase):

    mock_dbclient_config = None
    mock_dbclient_factory = None
    mock_annotation = None
    mock_limit = None
    mock_base = None

    annotation = {
        "time": "2018-08-24 12:03:04-0800",
        "id": "Annotation_1",
        "description": "Description of annotation 1"
    }

    limit = {
        "id": "Limit_1",
        "description": "Limit that indicates x",
        "value": 30000
    }

    EXISTS = False

    @patch(PACKAGE + ".Annotation")
    def test_add_annotation_exists(self, mock_annotation, mock_base,
                                   mock_dbclient_config, mock_db_client_factory):
        TestJsonLoaderDao.setMockBase(mock_base, mock_dbclient_config, mock_db_client_factory)
        TestJsonLoaderDao.setMockAnnotation(mock_annotation)
        with patch(PACKAGE + ".session_scope") as session:
            session.return_value.__enter__.return_value.query().filter_by().first.return_value = "dummy_annotation"
            dao = JsonDao()
            dao.add_annotation(TestJsonLoaderDao.annotation, None, None)
            self.assertEquals(mock_annotation.update.call_count, 1)

    @patch(PACKAGE + ".Annotation")
    def test_add_annotation_not_exists(self, mock_annotation, mock_base,
                                       mock_dbclient_config, mock_dbclient_factory):
        TestJsonLoaderDao.setMockBase(mock_base, mock_dbclient_config, mock_dbclient_factory)
        TestJsonLoaderDao.setMockAnnotation(mock_annotation)
        with patch(PACKAGE + ".session_scope") as session:
            session.return_value.__enter__.return_value.query().filter_by().first.return_value = None
            dao = JsonDao()
            dao.add_annotation(TestJsonLoaderDao.annotation, None, None)
            self.assertEquals(mock_annotation.update.call_count, 0)

    @patch(PACKAGE + ".Limit")
    def test_add_limit_exists(self, mock_limit, mock_base, mock_dbclient_config, mock_dbclient_factory):
        TestJsonLoaderDao.setMockBase(mock_base, mock_dbclient_config, mock_dbclient_factory)
        TestJsonLoaderDao.setMockLimit(mock_limit)
        with patch(PACKAGE + ".session_scope") as session:
            session.return_value.__enter__.return_value.query().filter_by().first.return_value = "dummy_limit"
            dao = JsonDao()
            dao.add_limit(TestJsonLoaderDao.limit, None)
            self.assertEquals(mock_limit.update.call_count, 1)

    @patch(PACKAGE + ".Limit")
    def test_add_limit_not_exists(self, mock_limit, mock_base, mock_dbclient_config, mock_dbclient_factory):
        TestJsonLoaderDao.setMockBase(mock_base, mock_dbclient_config, mock_dbclient_factory)
        TestJsonLoaderDao.setMockLimit(mock_limit)
        with patch(PACKAGE + ".session_scope") as session:
            session.return_value.__enter__.return_value.query().filter_by().first.return_value = None
            dao = JsonDao()
            dao.add_limit(TestJsonLoaderDao.limit, None)
            self.assertEquals(mock_limit.update.call_count, 0)

    @classmethod
    def setMockBase(cls, mock_base, mock_dbclient_config, mock_dbclient_factory):
        if not TestJsonLoaderDao.mock_base:
            TestJsonLoaderDao.mock_base = mock_base
            TestJsonLoaderDao.mock_base.reset_mock()
        if not TestJsonLoaderDao.mock_dbclient_config:
            TestJsonLoaderDao.mock_dbclient_config = mock_dbclient_config
            TestJsonLoaderDao.mock_dbclient_config.reset_mock()
        if not TestJsonLoaderDao.mock_dbclient_factory:
            TestJsonLoaderDao.mock_dbclient_factory = mock_dbclient_factory
            TestJsonLoaderDao.mock_dbclient_factory.reset_mock()

    @classmethod
    def setMockAnnotation(cls, mock_annotation):
        if not TestJsonLoaderDao.mock_annotation:
            TestJsonLoaderDao.mock_annotation = mock_annotation
            TestJsonLoaderDao.mock_annotation.reset_mock()

    @classmethod
    def setMockLimit(cls, mock_limit):
        if not TestJsonLoaderDao.mock_limit:
            TestJsonLoaderDao.mock_limit = mock_limit
            TestJsonLoaderDao.mock_limit.reset_mock()
