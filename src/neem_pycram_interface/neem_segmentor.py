import queue
import threading
import time
from abc import ABC, abstractmethod

import numpy as np
from tf.transformations import quaternion_inverse, quaternion_multiply
from typing_extensions import Optional, List, Union, Dict, Tuple

from pycram.datastructures.dataclasses import ContactPoint, ContactPointsList, Color
from pycram.datastructures.enums import WorldMode
from pycram.datastructures.pose import Transform
from pycram.world import World
from pycram.world_concepts.world_object import Object, Link
from pycram.worlds.bullet_world import BulletWorld
from .neem_pycram_interface import PyCRAMNEEMInterface


class NEEMSegmentor:

    def __init__(self, pycram_neem_interface: PyCRAMNEEMInterface):
        self.pni = pycram_neem_interface
        self.world = BulletWorld(mode=WorldMode.GUI)

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
    def __init__(self, contact_points: ContactPointsList, timestamp: Optional[float] = None):
        super().__init__(timestamp)
        self.contact_points = contact_points

    def object_names_in_contact(self):
        return self.contact_points.get_names_of_objects_that_have_points()

    def __str__(self):
        return f"Contact {self.contact_points[0].link_a.object.name}: {self.object_names_in_contact()}"

    def __repr__(self):
        return self.__str__()


class LossOfContactEvent(Event):
    def __init__(self, contact_points: ContactPointsList, latest_contact_points: ContactPointsList,
                 timestamp: Optional[float] = None):
        super().__init__(timestamp)
        self.contact_points = contact_points
        self.latest_contact_points = latest_contact_points

    def object_names_lost_contact(self):
        return [obj.name for obj in self.contact_points.get_objects_that_got_removed(self.latest_contact_points)]

    def objects_lost_contact(self):
        return self.contact_points.get_objects_that_got_removed(self.latest_contact_points)

    def __str__(self):
        return f"Loss of contact {self.latest_contact_points[0].link_a.object.name}: {self.object_names_lost_contact()}"

    def __repr__(self):
        return self.__str__()


class PickUpEvent(Event):
    def __init__(self, hand: Object, picked_object: Object, timestamp: Optional[float] = None):
        super().__init__(timestamp)
        self.hand = hand
        self.object = picked_object
        self.end_timestamp: Optional[float] = None

    def record_end_timestamp(self):
        self.end_timestamp = time.time()

    def duration(self):
        if self.end_timestamp is None:
            return None
        return self.end_timestamp - self.timestamp

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

    def get_events(self) -> Dict[str, Event]:
        with self.lock:
            events = self.timeline.copy()
        return events

    def get_latest_event_of_thread(self, thread_id: str):
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

    def query_neems_motion_replay_data(self, sql_neem_ids: List[int]):
        self.pni.query_neems_motion_replay_data(sql_neem_ids=sql_neem_ids)

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

    def __init__(self, logger: EventLogger,
                 wait_time: Optional[float] = None):
        """
        :param logger: An instance of the EventLogger class that is used to log the events.
        :param wait_time: An optional float value that introduces a delay between calls to the event detector.
        """

        super().__init__()

        self.logger = logger
        self.wait_time = wait_time

        self.exit_thread: Optional[bool] = False
        self.run_once = False

    @property
    @abstractmethod
    def thread_id(self) -> str:
        """
        A string that identifies the thread.
        """
        pass

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


class AbstractContactDetector(EventDetector, ABC):
    def __init__(self, logger: EventLogger,
                 object_to_track: Object,
                 with_object: Optional[Object] = None,
                 max_closeness_distance: Optional[float] = 0.05,
                 wait_time: Optional[float] = 0.1):
        """
        :param logger: An instance of the EventLogger class that is used to log the events.
        :param object_to_track: An instance of the Object class that represents the object to track.
        :param max_closeness_distance: An optional float value that represents the maximum distance between the object
        :param wait_time: An optional float value that introduces a delay between calls to the event detector.
        """
        super().__init__(logger, wait_time)
        self.object_to_track = object_to_track
        self.with_object = with_object
        self.max_closeness_distance = max_closeness_distance
        self.latest_contact_points: Optional[ContactPointsList] = ContactPointsList([])

    def detect_event(self) -> Union[Event, None]:
        """
        Detects the closest points between the object to track and another object in the scene if the with_object
        attribute is set, else, between the object to track and all other objects in the scene.
        """
        contact_points = self.get_contact_points()

        event = self.trigger_event(contact_points)

        self.latest_contact_points = contact_points

        return event

    def get_contact_points(self) -> ContactPointsList:
        if self.with_object is not None:
            contact_points = self.object_to_track.closest_points_with_obj(self.with_object,
                                                                          self.max_closeness_distance)
        else:
            contact_points = self.object_to_track.closest_points(self.max_closeness_distance)
        return contact_points

    @abstractmethod
    def trigger_event(self, contact_points: ContactPointsList) -> Union[Event, None]:
        """
        Checks if the detection condition is met, (e.g., the object is in contact with another object),
        and returns an object that represents the event.
        :param contact_points: The current contact points.
        :return: An object that represents the event.
        """
        pass


class ContactDetector(AbstractContactDetector):
    """
    A thread that detects if the object got into contact with another object.
    """

    thread_prefix = "contact_"
    """
    A string that is used as a prefix for the thread ID.
    """

    @property
    def thread_id(self) -> str:
        return self.thread_prefix + str(self.object_to_track.id)

    def trigger_event(self, contact_points: ContactPointsList) -> Union[ContactEvent, None]:
        """
        Check if the object got into contact with another object.
        :param contact_points:
        :return:
        """
        new_objects_in_contact = contact_points.get_new_objects(self.latest_contact_points)
        if len(new_objects_in_contact) == 0:
            return None
        return ContactEvent(contact_points)


class LossOfContactDetector(AbstractContactDetector):
    """
    A thread that detects if the object lost contact with another object.
    """

    thread_prefix = "loss_contact_"
    """
    A string that is used as a prefix for the thread ID.
    """

    @property
    def thread_id(self) -> str:
        return self.thread_prefix + str(self.object_to_track.id)

    def trigger_event(self, contact_points: ContactPointsList) -> Union[LossOfContactEvent, None]:
        """
        Check if the object lost contact with another object.
        :param contact_points:
        :return:
        """
        objects_that_lost_contact = contact_points.get_objects_that_got_removed(self.latest_contact_points)
        if len(objects_that_lost_contact) == 0:
            return None
        return LossOfContactEvent(contact_points, self.latest_contact_points)


class PickUpDetector(EventDetector):
    """
    A thread that detects if the object was picked up by the hand.
    """

    thread_prefix = "pick_up_"
    """
    A string that is used as a prefix for the thread ID.
    """

    def __init__(self, logger: EventLogger,
                 hand_link: Link,
                 object_link: Link,
                 trans_threshold: Optional[float] = 0.08,
                 rot_threshold: Optional[float] = 0.4
                 ):
        """
        :param logger: An instance of the EventLogger class that is used to log the events.
        :param hand_link: An instance of the Link class that represents the hand link.
        :param object_link: An instance of the Link class that represents the object link.
        :param trans_threshold: An optional float value that represents the translation threshold.
        :param rot_threshold: An optional float value that represents the rotation threshold.
        """
        super().__init__(logger)
        self.hand_link = hand_link
        self.hand = hand_link.object
        self.hand_name = hand_link.object.name
        self.object_link = object_link
        self.object = object_link.object
        self.trans_threshold = trans_threshold
        self.rot_threshold = rot_threshold
        self.run_once = True

    @property
    def thread_id(self) -> str:
        return self.thread_prefix + str(self.object_link.object.id)

    def detect_event(self) -> Optional[PickUpEvent]:
        """
        Detects if the object was picked up by the hand.
        Used Features are:
        1. The hand should still be in contact with the object.
        2. While the object that is picked should lose contact with the surface.
        Other features that can be used: Grasping Type, Object Type, and Object Motion.
        :return: An instance of the PickUpEvent class that represents the event if the object was picked up, else None.
        """

        # detect all their contacts at the time of contact with each other.
        initial_contact_event = self.get_latest_contact_event(self.object)
        initial_contact_points = initial_contact_event.contact_points
        if not initial_contact_points.is_object_in_the_list(self.hand):
            print(f"Hand not in contact with object: {self.object_link.object.name}")
            return

        pick_up_event = PickUpEvent(self.hand_link.object, self.object_link.object, initial_contact_event.timestamp)
        latest_stamp = pick_up_event.timestamp

        supporting_surface_found = False
        while not supporting_surface_found:
            time.sleep(0.01)
            loss_of_contact_event = self.get_latest_loss_of_contact_event(self.object, latest_stamp)
            loss_of_contact_points = loss_of_contact_event.contact_points

            objects_that_lost_contact = loss_of_contact_points.get_objects_that_got_removed(initial_contact_points)
            print(f"object_in_contact: {self.object.name}")
            if len(objects_that_lost_contact) == 0:
                print(f"continue, object: {self.object.name}")
                continue
            if self.hand in objects_that_lost_contact:
                print(f"Hand lost contact with object: {self.object_link.object.name}")
                return

            supporting_surface_found = self.check_for_supporting_surface(objects_that_lost_contact,
                                                                         initial_contact_points)
            if supporting_surface_found:
                pick_up_event.record_end_timestamp()
                break
            else:
                print(f"Supporting surface not found, object: {self.object.name}")
                continue

        print(f"Object picked up: {self.object_link.object.name}")

        return pick_up_event

    def check_for_supporting_surface(self, objects: List[Object], contact_points: List[ContactPoint]) -> bool:
        """
        Checks if any of the objects in the list are supporting surfaces.
        :param objects: An instance of the Object class that represents the object to check.
        :param contact_points: A list of ContactPoint instances that represent the contact points of the object.
        :return: A boolean value that represents the condition for the object to be considered as a supporting surface.
        """
        supporting_surface = None
        opposite_gravity = [0, 0, 9.81]
        smallest_angle = np.pi / 4
        for obj in objects:
            normals = contact_points.get_normals_of_object(obj)
            for normal in normals:
                # check if normal is pointing upwards opposite to gravity by finding the angle between the normal
                # and gravity vector.
                angle = self.get_angle_between_vectors(normal, opposite_gravity)
                if angle < smallest_angle:
                    smallest_angle = angle
                    supporting_surface = obj
        supporting_surface_cond = supporting_surface is not None
        return supporting_surface_cond

    @staticmethod
    def get_angle_between_vectors(vector_1: List[float], vector_2: List[float]) -> float:
        """
        Gets the angle between two vectors.
        :param vector_1: A list of float values that represent the first vector.
        :param vector_2: A list of float values that represent the second vector.
        :return: A float value that represents the angle between the two vectors.
        """
        return np.arccos(np.dot(vector_1, vector_2) / (np.linalg.norm(vector_1) * np.linalg.norm(vector_2)))

    def get_latest_contact_points(self, obj: Object) -> ContactPointsList:
        """
        Gets the latest contact points from the logger.
        :param obj: An instance of the Object class that represents the object to get the contact points from.
        :return: A list of ContactPoint instances that represent the contact points.
        """
        return self.get_latest_contact_event(obj).contact_points

    def get_latest_contact_event_between_hand_and_obj(self) -> ContactEvent:
        """
        Gets the latest contact event from the logger.
        :return: An instance of the ContactEvent class that represents the contact event.
        """
        hand_thread_id = ContactDetector.thread_prefix + str(self.hand.id)
        all_events = self.logger.get_events()
        latest_contact_event = None
        for event in reversed(all_events[hand_thread_id]):
            if event.contact_points.is_object_in_the_list(self.object):
                latest_contact_event = event
                break
        assert latest_contact_event is not None, f"No contact event for {self.hand.name} and {self.object.name}"
        return latest_contact_event

    def get_latest_contact_event(self, obj: Object) -> ContactEvent:
        """
        Gets the latest contact event from the logger.
        :param obj: An instance of the Object class that represents the object to get the contact event from.
        :return: An instance of the ContactEvent class that represents the contact event.
        """
        latest_contact_event = None
        thread_id = ContactDetector.thread_prefix + str(obj.id)
        while latest_contact_event is None:
            latest_contact_event = self.logger.get_latest_event_of_thread(thread_id)
            time.sleep(0.01)
        return latest_contact_event

    def get_latest_loss_of_contact_points(self, obj: Object,
                                          after_timestamp: Optional[float] = 0) -> ContactPointsList:
        """
        Gets the latest loss of contact points from the logger.
        :param obj: An instance of the Object class that represents the object to get the loss of contact points from.
        :param after_timestamp: A float value that represents the timestamp to get the loss of contact points after.
        :return: A list of ContactPoint instances that represent the loss of contact points.
        """
        return self.get_latest_loss_of_contact_event(obj, after_timestamp).contact_points

    def get_latest_loss_of_contact_event(self, obj: Object, after_timestamp: Optional[float] = 0) -> LossOfContactEvent:
        """
        Gets the latest loss of contact event from the logger.
        :param obj: An instance of the Object class that represents the object to get the loss of contact event from.
        :param after_timestamp: A float value that represents the timestamp to get the loss of contact event after.
        :return: An instance of the LossOfContactEvent class that represents the loss of contact event.
        """
        latest_loss_contact_event = None
        thread_id = LossOfContactDetector.thread_prefix + str(obj.id)
        while latest_loss_contact_event is None:
            latest_loss_contact_event = self.logger.get_latest_event_of_thread(thread_id)
            if latest_loss_contact_event is not None:
                latest_loss_contact_event = None if latest_loss_contact_event.timestamp < after_timestamp else \
                    latest_loss_contact_event
            if latest_loss_contact_event is None:
                time.sleep(0.01)
        return latest_loss_contact_event

    def calculate_transform_difference_and_check_if_small(self, transform_1: Transform, transform_2: Transform) \
            -> bool:
        """
        Calculates the translation and rotation of the object with respect to the hand to check if it was picked up,
         uses the translation and rotation thresholds to determine if the object was picked up.
        :param transform_1: The transform of the object at the first time step.
        :param transform_2: The transform of the object at the second time step.
        :return: A tuple of two boolean values that represent the conditions for the translation and rotation of the
        object to be considered as picked up.
        """
        trans_1, quat_1 = transform_1.translation_as_list(), transform_1.rotation_as_list()
        trans_2, quat_2 = transform_2.translation_as_list(), transform_2.rotation_as_list()
        trans_diff_cond = self.calculate_translation_difference_and_check(trans_1, trans_2)
        rot_diff_cond = self.calculate_angle_between_quaternions_and_check(quat_1, quat_2)
        print(f"trans_diff_cond {trans_diff_cond}, rot_diff_cond {rot_diff_cond}")
        return trans_diff_cond and rot_diff_cond

    def calculate_translation_difference_and_check(self, trans_1: List[float], trans_2: List[float]) -> bool:
        """
        Calculates the translation difference and checks if it is small.
        :param trans_1: The translation of the object at the first time step.
        :param trans_2: The translation of the object at the second time step.
        :return: A boolean value that represents the condition for the translation of the object to be considered as
        picked up.
        """
        translation_diff = self.calculate_translation_difference(trans_1, trans_2)
        return self.is_translation_difference_small(translation_diff)

    def is_translation_difference_small(self, trans_diff: List[float]) -> bool:
        """
        Checks if the translation difference is small by comparing it to the translation threshold.
        """
        return all([diff <= self.trans_threshold for diff in trans_diff])

    @staticmethod
    def calculate_translation_difference(trans_1: List[float], trans_2: List[float]) -> List[float]:
        """
        Calculates the translation difference.
        :param trans_1: The translation of the object at the first time step.
        :param trans_2: The translation of the object at the second time step.
        :return: A list of float values that represent the translation difference.
        """
        return [abs(t1 - t2) for t1, t2 in zip(trans_1, trans_2)]

    def calculate_angle_between_quaternions_and_check(self, quat_1: List[float], quat_2: List[float]) -> bool:
        """
        Calculates the angle between two quaternions and checks if it is small.
        :param quat_1: The first quaternion.
        :param quat_2: The second quaternion.
        :return: A boolean value that represents the condition for the angle between the two quaternions
         to be considered as small.
        """
        quat_diff_angle = self.calculate_angle_between_quaternions(quat_1, quat_2)
        return quat_diff_angle <= self.rot_threshold

    def is_quaternion_angle_difference_small(self, quat_diff_angle: List[float]) -> bool:
        """
        Checks if the quaternion angle difference is small by comparing it to the rotation threshold.
        """
        return quat_diff_angle <= self.rot_threshold

    def calculate_angle_between_quaternions(self, quat_1: List[float], quat_2: List[float]) -> float:
        """
        Calculates the angle between two quaternions.
        :param quat_1: The first quaternion.
        :param quat_2: The second quaternion.
        :return: A float value that represents the angle between the two quaternions.
        """
        quat_diff = self.calculate_quaternion_difference(quat_1, quat_2)
        quat_diff_angle = 2 * np.arctan2(np.linalg.norm(quat_diff[0:3]), quat_diff[3])
        return quat_diff_angle

    @staticmethod
    def calculate_quaternion_difference(quat_1: List[float], quat_2: List[float]) -> List[float]:
        """
        Calculates the quaternion difference.
        :param quat_1: The quaternion of the object at the first time step.
        :param quat_2: The quaternion of the object at the second time step.
        :return: A list of float values that represent the quaternion difference.
        """
        quat_diff = quaternion_multiply(quaternion_inverse(quat_1), quat_2)
        return quat_diff

    def calc_hand_to_obj_transform(self) -> Transform:
        """
        Calculates the transform of the object with respect to the hand link.
        :return: An instance of the Transform class that represents the transform of the object with respect to the hand
        link.
        """
        return self.hand_link.get_transform_to_link(self.object_link)


def run_event_detectors_on_neem(sql_neem_ids: Optional[List[int]] = None):
    logger = EventLogger()
    pni = PyCRAMNEEMInterface('mysql+pymysql://newuser:password@localhost/test')
    neem_player_thread = NEEMPlayer(pni)
    if sql_neem_ids is None:
        sql_neem_ids = [17]
    neem_player_thread.query_neems_motion_replay_data(sql_neem_ids)
    neem_player_thread.start()

    while not neem_player_thread.ready:
        time.sleep(0.1)

    world: World = World.current_world

    hands: List[Object] = [obj for obj in World.current_world.objects if "hand" in obj.name.lower()]
    # kitchen = [obj for obj in World.current_world.objects if "kitchen" in obj.name.lower()][0]

    all_contact_events_to_look_for = [{'object_to_track': hand} for hand in hands]
    detector_threads = []
    tracked_objects = [event['object_to_track'] for event in all_contact_events_to_look_for]
    avoid_objects = ['particle', 'floor', 'kitchen']

    def start_contact_threads_for_obj(obj: Object):
        for detector in (ContactDetector, LossOfContactDetector):
            detector_thread = detector(logger, obj)
            detector_thread.start()
            detector_threads.append(detector_thread)
        tracked_objects.append(obj)

    for i, event in enumerate(all_contact_events_to_look_for):
        start_contact_threads_for_obj(event['object_to_track'])

    pick_up_detectors = {}

    def start_pick_up_thread_for_obj(hand_link: Link, obj_link: Link):
        obj = obj_link.object
        if obj in pick_up_detectors.keys():
            if (pick_up_detectors[obj].is_alive() or
                    pick_up_detectors[obj].thread_id in logger.get_events().keys()):
                return
        pick_up_detector = PickUpDetector(logger, hand_link, obj_link)
        pick_up_detector.start()
        pick_up_detectors[obj] = pick_up_detector
        detector_threads.append(pick_up_detector)
        print(f"Creating pick up detector for object {obj.name}")

    while neem_player_thread.is_alive() or logger.event_queue.unfinished_tasks > 0:
        thread_id, next_event = logger.get_next_event()
        if next_event is None:
            time.sleep(0.01)
            continue
        if isinstance(next_event, PickUpEvent):
            world.add_text(f"Picked {next_event.object.name}", next_event.hand.get_position_as_list(),
                           color=Color(1, 0, 0, 1))

        if not isinstance(next_event, ContactEvent):
            continue

        objects_in_contact = next_event.contact_points.get_objects_that_have_points()
        obj_a = next_event.contact_points[0].link_a.object
        link_a = next_event.contact_points[0].link_a
        for obj_in_contact in objects_in_contact:
            if any([k in obj_in_contact.name.lower() for k in avoid_objects]):
                continue

            link_b = next_event.contact_points.get_links_in_contact_of_object(obj_in_contact)[0]

            if obj_in_contact not in tracked_objects:
                print(f"Creating new thread for object {obj_in_contact.name}")
                start_contact_threads_for_obj(obj_in_contact)

            if obj_a in hands and obj_in_contact not in hands:
                start_pick_up_thread_for_obj(link_a, link_b)

    neem_player_thread.join()

    for detector_thread in detector_threads:
        detector_thread.exit_thread = True
        detector_thread.join()

    logger.join()

    print("All threads have exited")
    print("Events:")
    print('\n'.join([' '.join([str(v) for v in values]) for values in logger.get_events().values()]))


if __name__ == "__main__":
    run_event_detectors_on_neem()
