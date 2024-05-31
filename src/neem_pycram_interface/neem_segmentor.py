import queue
import threading
import time
from abc import ABC, abstractmethod

import numpy as np
from tf.transformations import quaternion_inverse, quaternion_multiply
from typing_extensions import Optional, List, Union, Dict

from pycram.datastructures.dataclasses import ContactPoint
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


class Event:
    def __init__(self, timestamp: Optional[float] = None):
        self.timestamp = time.time() if timestamp is None else timestamp


class ContactEvent(Event):
    def __init__(self, contact_points: List[ContactPoint], timestamp: Optional[float] = None):
        super().__init__(timestamp)
        self.contact_points = contact_points

    def __str__(self):
        return f"Contact event: {[str(cp) for cp in self.contact_points]}"

    def __repr__(self):
        return self.__str__()


class PickUpEvent(Event):
    def __init__(self, hand: Object, picked_object: Object, timestamp: Optional[float] = None):
        super().__init__(timestamp)
        self.hand = hand
        self.object = picked_object

    def __str__(self):
        return f"Pick up event: Hand:{self.hand.name}, Object: {self.object.name}, Timestamp: {self.timestamp}"

    def __repr__(self):
        return self.__str__()


class EventLogger:
    def __init__(self):
        self.timeline = {}
        self.event_queue = queue.Queue()
        self.lock = threading.Lock()

    def log_event(self, thread_id, event: Event):
        self.event_queue.put((thread_id, event))
        with self.lock:
            if thread_id not in self.timeline:
                self.timeline[thread_id] = []
            self.timeline[thread_id].append(event)

    def get_events(self) -> Dict[int, Event]:
        with self.lock:
            events = self.timeline.copy()
        return events

    def get_latest_event_of_thread(self, thread_id: int):
        with self.lock:
            if thread_id not in self.timeline:
                return None
            return self.timeline[thread_id][-1]

    def get_next_event(self):
        try:
            thread_id, event = self.event_queue.get(block=False)
            self.event_queue.task_done()
            return thread_id, event
        except queue.Empty:
            return None, None

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
        self.pni.replay_motions_in_query(real_time=True)
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
        self.run_once = False

    @abstractmethod
    def detect_event(self) -> Event:
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
            if self.run_once:
                break

    def log_event(self, event: Event) -> None:
        """
        Logs the event using the logger instance.
        :param event: An object that represents the event.
        :return: None
        """
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

    def detect_event(self) -> Union[ContactEvent, None]:
        """
        Detects the closest points between the object to track and another object in the scene if the with_object
        attribute is set, else, between the object to track and all other objects in the scene.
        """
        if self.with_object is not None:
            contact_points = self.object_to_track.closest_points_with_obj(self.with_object, self.max_closeness_distance)
        else:
            contact_points = self.object_to_track.closest_points(self.max_closeness_distance)
        if len(contact_points) > 0:
            return ContactEvent(contact_points)
        else:
            return None


class PickUpDetector(EventDetector):
    def __init__(self, thread_id: int,
                 logger: EventLogger,
                 hand_link: Link,
                 object_link: Link,
                 hand_contact_thread_id: int,
                 obj_contact_thread_id: int,
                 trans_threshold: Optional[float] = 0.08,
                 rot_threshold: Optional[float] = 0.4
                 ):
        """
        :param thread_id: An integer that identifies the thread.
        :param logger: An instance of the EventLogger class that is used to log the events.
        :param hand_link: An instance of the Link class that represents the hand link.
        :param object_link: An instance of the Link class that represents the object link.
        """
        super().__init__(thread_id, logger)
        self.hand_link = hand_link
        self.object_link = object_link
        self.hand_contact_thread_id = hand_contact_thread_id
        self.obj_contact_thread_id = obj_contact_thread_id
        self.trans_threshold = trans_threshold
        self.rot_threshold = rot_threshold
        self.run_once = True

    def detect_event(self):
        pick_up_event = PickUpEvent(self.hand_link.object, self.object_link.object)
        # measure translation, rotation between the two objects and detect all their contacts
        # at the time of contact with each other.
        transform_hand_obj = self.hand_link.get_transform_to_link(self.object_link)
        trans_1 = transform_hand_obj.translation_as_list()
        quat_1 = transform_hand_obj.rotation_as_list()
        initial_obj_contact_event = None
        while initial_obj_contact_event is None:
            initial_obj_contact_event = self.logger.get_latest_event_of_thread(self.obj_contact_thread_id)
            time.sleep(0.01)
        points_with_other_objects = [point for point in initial_obj_contact_event.contact_points
                                     if point.link_b.object != self.hand_link.object]
        objects_other_than_hand_at_contact = set([point.link_b.object.name for point in points_with_other_objects])

        # Do that again after some time.
        time.sleep(4)
        transform_hand_obj = self.hand_link.get_transform_to_link(self.object_link)
        trans_2 = transform_hand_obj.translation_as_list()
        quat_2 = transform_hand_obj.rotation_as_list()

        # Indicative conditions:

        # 1. the change in translation and rotation should be small if the object was picked up
        trans_diff_cond = all([abs(t1 - t2) <= self.trans_threshold for t1, t2 in zip(trans_1, trans_2)])
        quat_diff = quaternion_multiply(quaternion_inverse(quat_1), quat_2)
        quat_diff_angle = 2 * np.arctan2(np.linalg.norm(quat_diff[0:3]), quat_diff[3])
        rot_diff_cond = quat_diff_angle <= self.rot_threshold
        print(f"trans_diff_cond {trans_diff_cond}, rot_diff_cond {rot_diff_cond}")

        # 2. the contact should still be there
        latest_contact_event = self.logger.get_latest_event_of_thread(self.hand_contact_thread_id)
        contact_points = latest_contact_event.contact_points
        obj_in_contact = [point.link_b.object for point in contact_points]
        contact_cond = self.object_link.object in obj_in_contact
        print(f"contact_cond {contact_cond}")

        # 3. while the object that is picked should lose contact with the surface.
        new_obj_contact_event = self.logger.get_latest_event_of_thread(self.obj_contact_thread_id)
        new_points_with_other_objects = [point for point in new_obj_contact_event.contact_points
                                         if point.link_b.object != self.hand_link.object]
        new_objects_other_than_hand_at_contact = set(
            [point.link_b.object.name for point in new_points_with_other_objects])
        print(f"objects_other_than_hand_at_contact {objects_other_than_hand_at_contact}")
        print(f"new_objects_other_than_hand_at_contact {new_objects_other_than_hand_at_contact}")
        objects_that_lost_contact = objects_other_than_hand_at_contact - new_objects_other_than_hand_at_contact
        supporting_surface = None
        opposite_gravity = [0, 0, 9.81]
        smallest_angle = np.pi / 4
        for obj in objects_that_lost_contact:
            points_with_obj = [point for point in points_with_other_objects if point.link_b.object.name == obj]
            normals = [point.normal_on_b for point in points_with_obj]
            for normal in normals:
                # check if normal is pointing upwards opposite to gravity by finding the angle between the normal
                # and gravity vector.
                angle = np.arccos(np.dot(normal, opposite_gravity) /
                                  (np.linalg.norm(normal) * np.linalg.norm(opposite_gravity)))
                print(f"Angle between normal and gravity: {angle}")
                if angle < smallest_angle:
                    smallest_angle = angle
                    supporting_surface = obj
        supporting_surface_cond = supporting_surface is not None
        print(f"supporting_surface_cond {supporting_surface_cond}")

        pick_up_cond = trans_diff_cond and rot_diff_cond and contact_cond and supporting_surface_cond
        if pick_up_cond:
            print(f"Object picked up: {self.object_link.object.name}")
            return pick_up_event
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
    pick_up_thread_initialized = False
    while neem_player_thread.is_alive() or logger.event_queue.unfinished_tasks > 0:
        thread_id, next_event = logger.get_next_event()
        if next_event is None:
            time.sleep(0.01)
            continue
        if not isinstance(next_event, ContactEvent):
            continue
        for point in next_event.contact_points:
            obj_in_contact = point.link_b.object
            obj_a = point.link_a.object
            if (obj_in_contact not in tracked_objects and
                    all([k not in obj_in_contact.name.lower() for k in avoid_objects])):
                print(f"Creating new thread for object {obj_in_contact.name}")
                thread_id = len(contact_detector_threads)
                # print(f"Thread ID: {thread_id} for object {obj_in_contact.name}")
                detector_thread = ContactDetector(len(contact_detector_threads), logger, obj_in_contact)
                detector_thread.start()
                contact_detector_threads.append(detector_thread)
                tracked_objects.append(obj_in_contact)
            if (obj_a == hand and obj_in_contact in tracked_objects and obj_in_contact.name == 'SM_MilkPitcher_1' and
                    not pick_up_thread_initialized):
                obj_thread_id = tracked_objects.index(obj_in_contact)
                # print(f"Thread ID for object {obj_in_contact.name}: {obj_thread_id}")
                pick_up_detector = PickUpDetector(99,
                                                  logger, point.link_a, point.link_b,
                                                  0, obj_thread_id)
                pick_up_detector.start()
                print(f"Creating pick up detector for object {obj_in_contact.name}")
                pick_up_thread_initialized = True

    logger.join()
    neem_player_thread.join()

    for detector_thread in contact_detector_threads:
        detector_thread.exit_thread = True
        detector_thread.join()

    print("All threads have exited")
    print("Events:")
    print('\n'.join(logger.get_events()))


if __name__ == "__main__":
    run_event_detectors()
