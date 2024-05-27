from neem_pycram_interface.neem_pycram_interface import PyCRAMNEEMInterface
from neem_pycram_interface.neem_segmentor import NEEMSegmentor
from unittest import TestCase

from pycram.ros.viz_marker_publisher import VizMarkerPublisher


class TestNEEMSegmentor(TestCase):
    ns: NEEMSegmentor
    viz_mark_publisher: VizMarkerPublisher

    @classmethod
    def setUpClass(cls):
        pni = PyCRAMNEEMInterface('mysql+pymysql://newuser:password@localhost/test')
        cls.ns = NEEMSegmentor(pni)
        cls.viz_mark_publisher = VizMarkerPublisher()

    @classmethod
    def tearDownClass(cls):
        cls.viz_mark_publisher._stop_publishing()
        cls.ns.world.exit()

    def test_detect_contacts_from_neem_motion_replay(self):
        self.ns.detect_contacts_from_neem_motion_replay(14)
