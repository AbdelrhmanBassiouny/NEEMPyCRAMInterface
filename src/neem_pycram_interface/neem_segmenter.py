import threading
import time

from typing_extensions import Optional, List

from .neem_pycram_interface import PyCRAMNEEMInterface
from .episode_segmenter import EpisodeSegmenter


class NEEMSegmenter(EpisodeSegmenter):
    """
    The NEEMSegmenter class is used to segment the NEEMs motion replay data by using event detectors, such as contact,
    loss of contact, and pick up events.
    """

    def __init__(self, pycram_neem_interface: PyCRAMNEEMInterface,  annotate_events: bool = False):
        """
        Initializes the NEEMSegmenter class.
        :param pycram_neem_interface: The neem pycram interface object used to query the NEEMs motion replay data.
        """
        self.neem_player_thread = NEEMPlayer(pycram_neem_interface)
        super().__init__(self.neem_player_thread, annotate_events)

    def run_event_detectors_on_neem(self, sql_neem_ids: Optional[List[int]] = None) -> None:
        """
        Runs the event detectors on the NEEMs motion replay data.
        :param sql_neem_ids: An optional list of integer values that represent the SQL NEEM IDs.
        """
        if sql_neem_ids is None:
            sql_neem_ids = [17]

        self.query_neems_motion_replay_data_and_start_neem_player(sql_neem_ids)

        self.run_event_detectors(self.neem_player_thread)

    def query_neems_motion_replay_data_and_start_neem_player(self, sql_neem_ids: List[int]) -> None:
        """
        Queries the NEEMs motion replay data, starts the NEEM player thread, and waits until the NEEM player thread is
        ready (i.e., the replay environment is initialized with all objects in starting poses).
        :param sql_neem_ids: A list of integer values that represent the SQL NEEM IDs.
        """
        self.neem_player_thread.query_neems_motion_replay_data(sql_neem_ids)
        self.neem_player_thread.start()
        while not self.neem_player_thread.ready:
            time.sleep(0.1)


class NEEMPlayer(threading.Thread):
    def __init__(self, pycram_neem_interface: PyCRAMNEEMInterface):
        super().__init__()
        self.pni = pycram_neem_interface

    def query_neems_motion_replay_data(self, sql_neem_ids: List[int]):
        self.pni.query_neems_motion_replay_data(sql_neem_ids=sql_neem_ids)

    @property
    def ready(self):
        return self.pni.replay_environment_initialized

    def run(self):
        self.pni.replay_motions_in_query(real_time=True)
