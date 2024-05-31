import threading
import queue
import time
from abc import ABC, abstractmethod
from datetime import datetime

import pybullet
from typing_extensions import Callable, Optional, Any, List, Union

from pycram.datastructures.dataclasses import ClosestPoint, ContactPoint
from pycram.datastructures.enums import WorldMode
from pycram.world import World
from pycram.world_concepts.world_object import Object, Link
from pycram.worlds.bullet_world import BulletWorld
from .neem_pycram_interface import PyCRAMNEEMInterface


class NEEMSegmentor:

    def __init__(self, pycram_neem_interface: PyCRAMNEEMInterface):
        self.pni = pycram_neem_interface
        self.world = BulletWorld(mode=WorldMode.DIRECT)

    def detect_contacts_from_neem_motion_replay(self, sql_neem_id: int):
        """
        Detects contacts between the hand and objects in the scene during the replay of a NEEM motion.
        :param sql_neem_id: The ID of the NEEM in the SQL database.
        """
        self.pni.query_neems_motion_replay_data(sql_neem_id=sql_neem_id)
        self.pni.replay_motions_in_query()
        while not self.pni.replay_environment_initialized:
            time.sleep(0.1)
        hand_obj = [obj for obj in self.world.objects if "hand" in obj.name.lower()][0]
        while self.pni.replay_environment_initialized:
            contact_points = hand_obj.closest_points(0.03)
            for contact_point in contact_points:
                print(f"Contact point: {contact_point}")
                obj_in_contact = self.world.get_object_by_id(contact_point[2])
                print(f"Hand is in contact with {obj_in_contact.name}")
                print(f"Object position: {obj_in_contact.get_position()}")


class EventLogger:
    def __init__(self):
        self.timeline = []
        self.event_queue = queue.Queue()
        self.lock = threading.Lock()

    def log_event(self, thread_id, event):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        if isinstance(event, list):
            log_event = [str(e) for e in event]
        else:
            log_event = event
        log_entry = f"Thread {thread_id} detected {log_event} at {timestamp}"
        self.event_queue.put(event)
        with self.lock:
            self.timeline.append(log_entry)

    def get_events(self):
        with self.lock:
            events = self.timeline[:]
        return events

    def get_next_event(self):
        try:
            event = self.event_queue.get(block=False)
            self.event_queue.task_done()
            return event
        except queue.Empty:
            return None

    def join(self):
        self.event_queue.join()


class NEEMPlayer(threading.Thread):
    def __init__(self, pycram_neem_interface: PyCRAMNEEMInterface):
        super().__init__()
        self.pni = pycram_neem_interface
        self.world = BulletWorld(mode=WorldMode.DIRECT)

    def query_neems_motion_replay_data(self, sql_neem_id: int):
        self.pni.query_neems_motion_replay_data(sql_neem_id=sql_neem_id)

    @property
    def ready(self):
        return self.pni.replay_environment_initialized

    def run(self):
        self.pni.replay_motions_in_query(real_time=False)
        self.world.exit()


class EventDetector(threading.Thread, ABC):
    """
    A thread that detects events in another thread and logs them. The event detector is a function that has no arguments
    and returns an object that represents the event. The event detector is called in a loop until the thread is stopped
    by setting the exit_thread attribute to True.
    """
    def __init__(self, thread_id: int, logger: EventLogger,
                 wait_time: Optional[float] = None):
        """
        :param thread_id: An integer that identifies the thread.
        :param logger: An instance of the EventLogger class that is used to log the events.
        :param wait_time: An optional float value that introduces a delay between calls to the event detector.
        """

        super().__init__()

        self.thread_id = thread_id
        self.logger = logger
        self.wait_time = wait_time

        self.exit_thread: Optional[bool] = False

    @abstractmethod
    def detect_event(self) -> Any:
        """
        The event detector function that is called in a loop until the thread is stopped.
        :return: An object that represents the event.
        """
        pass

    def run(self):
        """
        The main loop of the thread. The event detector is called in a loop until the thread is stopped by setting the
        exit_thread attribute to True. Additionally, there is an optional wait_time attribute that can be set to a float
        value to introduce a delay between calls to the event detector.
        """
        while not self.exit_thread:
            event = self.detect_event()
            if self.wait_time is not None:
                time.sleep(self.wait_time)
            if event is not None:
                self.log_event(event)

    def log_event(self, event: Any) -> None:
        """
        Logs the event using the logger instance.
        :param event: An object that represents the event.
        :return: None
        """
        # print(f"Thread {self.thread_id} detected event: {event}")
        self.logger.log_event(self.thread_id, event)


class ContactDetector(EventDetector):
    def __init__(self, thread_id: int,
                 logger: EventLogger,
                 object_to_track: Object,
                 with_object: Optional[Object] = None,
                 max_closeness_distance: Optional[float] = 0.03,
                 wait_time: Optional[float] = 0.1):
        """
        :param thread_id: An integer that identifies the thread.
        :param logger: An instance of the EventLogger class that is used to log the events.
        :param object_to_track: An instance of the Object class that represents the object to track.
        :param max_closeness_distance: An optional float value that represents the maximum distance between the object
        :param wait_time: An optional float value that introduces a delay between calls to the event detector.
        to track and another object in the scene to consider them in contact.
        """
        super().__init__(thread_id, logger, wait_time)
        self.object_to_track = object_to_track
        self.with_object = with_object
        self.max_closeness_distance = max_closeness_distance

    def detect_event(self) -> Union[List[ClosestPoint], None]:
        """
        Detects the closest points between the object to track and another object in the scene if the with_object
        attribute is set, else, between the object to track and all other objects in the scene.
        """
        if self.with_object is not None:
            contact_points = self.object_to_track.closest_points_with_obj(self.with_object, self.max_closeness_distance)
        else:
            contact_points = self.object_to_track.closest_points(self.max_closeness_distance)
        if len(contact_points) > 0:
            return contact_points
        else:
            return None


def run_event_detectors():

    logger = EventLogger()
    pni = PyCRAMNEEMInterface('mysql+pymysql://newuser:password@localhost/test')
    neem_player_thread = NEEMPlayer(pni)
    neem_player_thread.query_neems_motion_replay_data(17)
    neem_player_thread.start()

    while not neem_player_thread.ready:
        time.sleep(0.1)

    hand = [obj for obj in World.current_world.objects if "hand" in obj.name.lower()][0]
    all_contact_events_to_look_for = [{'object_to_track': hand}]
    contact_detector_threads = []
    for i, event in enumerate(all_contact_events_to_look_for):
        detector_thread = ContactDetector(i, logger, **event)
        detector_thread.start()
        contact_detector_threads.append(detector_thread)

    tracked_objects = [event['object_to_track'] for event in all_contact_events_to_look_for]
    avoid_objects = ['particle', 'floor', 'kitchen']
    while neem_player_thread.is_alive() or logger.event_queue.unfinished_tasks > 0:
        next_event = logger.get_next_event()
        if next_event is None:
            time.sleep(0.01)
            continue
        for point in next_event:
            obj_in_contact = point.link_b.object
            if (obj_in_contact not in tracked_objects and
                    all([k not in obj_in_contact.name.lower() for k in avoid_objects])):
                print(f"Creating new thread for object {obj_in_contact.name}")
                detector_thread = ContactDetector(len(contact_detector_threads), logger, obj_in_contact)
                detector_thread.start()
                contact_detector_threads.append(detector_thread)
                tracked_objects.append(obj_in_contact)

    logger.join()
    neem_player_thread.join()

    for detector_thread in contact_detector_threads:
        detector_thread.exit_thread = True
        detector_thread.join()

    print("All threads have exited")
    print("Events:")
    print(logger.get_events())


if __name__ == "__main__":
    run_event_detectors()


