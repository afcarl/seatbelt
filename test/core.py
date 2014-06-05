# Tests are handy.

import json
import multiprocessing
import unittest
import urlparse
import requests
import shutil
import tempfile

from seatbelt.seatbelt import serve

class SafetyBelt(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        q = multiprocessing.Queue()

        self.db_proc = multiprocessing.Process(target=serve, 
                                               args=(self.tmpdir,), 
                                               kwargs={"queue":q})
        self.db_proc.start()

        self.root_uri = q.get()

    def get_json(self, url):
        res = requests.get(url)
        doc = json.loads(res.text)
        return doc

    def test_existence(self):
        doc = self.get_json(self.root_uri)
        self.assertEqual(doc["db"], "seatbelt")
        self.assertEqual(doc["version"], -1)

    def test_no_dbs(self):
        doc = self.get_json(urlparse.urljoin(self.root_uri, "_all_dbs"))
        self.assertEqual(doc, [])

    def tearDown(self):
        self.db_proc.terminate()
        shutil.rmtree(self.tmpdir)

if __name__=='__main__':
    unittest.main()
