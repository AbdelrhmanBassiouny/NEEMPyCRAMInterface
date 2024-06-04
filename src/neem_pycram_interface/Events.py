from typing_extensions import Optional, List, Dict
import queue
import threading
import time

from pycram.world_concepts.world_object import Object
from pycram.world import World
from pycram.datastructures.dataclasses import ContactPointsList, Color


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

    def annotate(self):
        color = Color(0, 1, 0)
        self.hand.set_color(color)
        self.object.set_color(color)
        World.current_world.add_text(f"Picked {self.object.name}",
                                     self.hand.get_position_as_list(),
                                     color=Color(1, 0, 0, 1))

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

    def print_events(self):
        print("Events:")
        print(self)

    def get_events(self) -> Dict[str, List[Event]]:
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

    def __str__(self):
        return '\n'.join([' '.join([str(v) for v in values]) for values in self.get_events().values()])
