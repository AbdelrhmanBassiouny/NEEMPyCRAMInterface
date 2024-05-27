from unittest import TestCase
from neem_query.neem_query import NeemQuery
from neem_pycram_interface.utils import RepositorySearch


class TestUtils(TestCase):
    nq: NeemQuery
    sr: RepositorySearch

    @classmethod
    def setUpClass(cls):
        cls.nq = NeemQuery('mysql+pymysql://newuser:password@localhost/test')
        cls.sr = RepositorySearch(cls.nq.neem_data_link, start_search_in=cls.nq._get_mesh_links())

    def test_get_links_from_page(self):
        links = self.sr.get_links_from_page(self.sr.repository_url)
        self.assertIsInstance(links, list)
        self.assertTrue(all(isinstance(link, str) for link in links))

    def test_search_similar_file_names(self):
        query = 'cup'
        file_names = self.sr.search_similar_file_names([query], find_all=False)
        self.assertTrue(len(self.sr.all_file_names) > 0)
        self.assertTrue(len(self.sr.all_file_links) > 0)
        self.assertTrue(all(isinstance(name, str) for name in self.sr.all_file_names))
        self.assertTrue(all(isinstance(link, str) for link in self.sr.all_file_links))
        self.assertTrue(query in file_names[0].lower())
