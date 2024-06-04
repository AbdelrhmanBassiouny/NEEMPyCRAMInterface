import threading
import time

from typing_extensions import Optional, List

from pycram.datastructures.enums import WorldMode
from pycram.world import World
from pycram.world_concepts.world_object import Object, Link
from pycram.worlds.bullet_world import BulletWorld
from .neem_pycram_interface import PyCRAMNEEMInterface

from .Events import ContactEvent, PickUpEvent, EventLogger
from .EventDetectors import ContactDetector, LossOfContactDetector, PickUpDetector


class NEEMSegmentor:
    """
    The NEEMSegmentor class is used to segment the NEEMs motion replay data by using event detectors, such as contact,
    loss of contact, and pick up events.
    """

    def __init__(self, pycram_neem_interface: PyCRAMNEEMInterface, annotate_events: bool = False):
        """
        Initializes the NEEMSegmentor class.
        :param pycram_neem_interface: The neem pycram interface object used to query the NEEMs motion replay data.
        """
        self.pni = pycram_neem_interface
        self.world = BulletWorld(mode=WorldMode.GUI)
        self.avoid_objects = ['particle', 'floor', 'kitchen']
        self.tracked_objects = []
        self.detector_threads = []
        self.logger = EventLogger(annotate_events)
        self.neem_player_thread = NEEMPlayer(self.pni)
        self.pick_up_detectors = {}

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

    def run_event_detectors_on_neem(self, sql_neem_ids: Optional[List[int]] = None) -> None:
        """
        Runs the event detectors on the NEEMs motion replay data.
        :param sql_neem_ids: An optional list of integer values that represent the SQL NEEM IDs.
        """
        if sql_neem_ids is None:
            sql_neem_ids = [17]

        self.query_neems_motion_replay_data_and_start_neem_player(sql_neem_ids)

        hands = self.get_hands_and_track_hand_contacts()

        while self.neem_player_thread.is_alive() or self.logger.event_queue.unfinished_tasks > 0:

            thread_id, next_event = self.logger.get_next_event()

            if next_event is None:
                time.sleep(0.01)
                continue

            if not isinstance(next_event, ContactEvent):
                continue

            self.handle_contact_event(next_event, hands)

        self.join()
        self.logger.print_events()

    def handle_contact_event(self, event: ContactEvent, hands: List[Object]) -> None:
        """
        Handles the contact event by starting the contact threads for the object and the pickup thread for the object.
        :param event: The ContactEvent instance that represents the contact event.
        :param hands: A list of Object instances that represent the hands.
        """
        objects_in_contact = event.contact_points.get_objects_that_have_points()
        obj_a = event.contact_points[0].link_a.object
        link_a = event.contact_points[0].link_a
        for obj_in_contact in objects_in_contact:
            if any([k in obj_in_contact.name.lower() for k in self.avoid_objects]):
                continue

            link_b = event.contact_points.get_links_in_contact_of_object(obj_in_contact)[0]

            if obj_in_contact not in self.tracked_objects:
                print(f"Creating new thread for object {obj_in_contact.name}")
                self.start_contact_threads_for_obj_and_update_tracked_objs(obj_in_contact)

            if obj_a in hands and obj_in_contact not in hands:
                self.start_pick_up_thread_for_obj(link_a, link_b)

    def start_contact_threads_for_obj_and_update_tracked_objs(self, obj: Object):
        """
        Starts the contact threads for the object and updates the tracked objects.
        :param obj: The Object instance for which the contact threads are started.
        """
        for detector in (ContactDetector, LossOfContactDetector):
            detector_thread = detector(self.logger, obj)
            detector_thread.start()
            self.detector_threads.append(detector_thread)
        self.tracked_objects.append(obj)

    def start_pick_up_thread_for_obj(self, hand_link: Link, obj_link: Link):
        """
        Starts the pickup thread for the object.
        :param hand_link: The Link instance that represents the hand.
        :param obj_link: The Link instance that represents the object.
        """
        obj = obj_link.object
        if obj in self.pick_up_detectors.keys():
            if (self.pick_up_detectors[obj].is_alive() or
                    self.pick_up_detectors[obj].thread_id in self.logger.get_events().keys()):
                return
        pick_up_detector = PickUpDetector(self.logger, hand_link, obj_link)
        pick_up_detector.start()
        self.pick_up_detectors[obj] = pick_up_detector
        self.detector_threads.append(pick_up_detector)
        print(f"Creating pick up detector for object {obj.name}")

    def get_hands_and_track_hand_contacts(self) -> List[Object]:
        """
        Gets the hands from the world and starts the contact threads for the hands.
        :return: A list of Object instances that represent the hands.
        """
        hands = self.get_hands()
        for hand in hands:
            self.start_contact_threads_for_obj_and_update_tracked_objs(hand)
        return hands

    @staticmethod
    def get_hands() -> List[Object]:
        """
        Gets the hands from the world.
        :return: A list of Object instances that represent the hands.
        """
        return [obj for obj in World.current_world.objects if 'hand' in obj.name.lower()]

    def join(self):
        """
        Joins all the threads.
        """
        self.neem_player_thread.join()

        for detector_thread in self.detector_threads:
            detector_thread.exit_thread = True
            detector_thread.join()

        self.logger.join()


class NEEMPlayer(threading.Thread):
    def __init__(self, pycram_neem_interface: PyCRAMNEEMInterface):
        super().__init__()
        self.pni = pycram_neem_interface
        self.world = BulletWorld(mode=WorldMode.DIRECT)

    def query_neems_motion_replay_data(self, sql_neem_ids: List[int]):
        self.pni.query_neems_motion_replay_data(sql_neem_ids=sql_neem_ids)

    @property
    def ready(self):
        return self.pni.replay_environment_initialized

    def run(self):
        self.pni.replay_motions_in_query(real_time=True)
        self.world.exit()
