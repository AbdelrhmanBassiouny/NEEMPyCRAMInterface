import queue
import threading
import time

from typing_extensions import Optional, List, Dict

from pycram.datastructures.dataclasses import ContactPointsList, Color, TextAnnotation
from pycram.world import World
from pycram.world_concepts.world_object import Object, Link


class Event:
    def __init__(self, timestamp: Optional[float] = None):
        self.timestamp = time.time() if timestamp is None else timestamp


class ContactEvent(Event):
    def __init__(self, contact_points: ContactPointsList, timestamp: Optional[float] = None):
        super().__init__(timestamp)
        self.contact_points = contact_points
        self.text_id: Optional[int] = None

    def object_names_in_contact(self):
        return self.contact_points.get_names_of_objects_that_have_points()

    def objects_in_contact(self):
        return self.contact_points.get_objects_that_have_points()

    def link_names_in_contact(self):
        return [link.name for link in self.links_in_contact()]

    def links_in_contact(self) -> List[Link]:
        links = []
        for obj in self.objects_in_contact():
            links.extend(self.contact_points.get_links_in_contact_of_object(obj))
        return links

    def annotate(self, position: Optional[List[float]] = None) -> TextAnnotation:
        if position is None:
            position = [2, 1, 2]
        color = Color(0, 0, 1, 1)
        main_link = self.contact_points[0].link_a
        main_link.color = color
        for link in self.links_in_contact():
            link.color = color
        text = f"{main_link.name} in contact with {self.link_names_in_contact()}"
        self.text_id = World.current_world.add_text(text,
                                                    position,
                                                    color=color)
        return TextAnnotation(text, position, color, self.text_id)

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
        self.text_id: Optional[int] = None

    def object_names_lost_contact(self):
        return [obj.name for obj in self.contact_points.get_objects_that_got_removed(self.latest_contact_points)]

    def link_names_lost_contact(self):
        return [link.name for link in self.links_lost_contact()]

    def links_lost_contact(self) -> List[Link]:
        objs_lost_contact = self.objects_lost_contact()
        links = []
        for obj in objs_lost_contact:
            links.extend(self.latest_contact_points.get_links_in_contact_of_object(obj))
        return links

    def objects_lost_contact(self):
        return self.contact_points.get_objects_that_got_removed(self.latest_contact_points)

    def annotate(self, position: Optional[List[float]] = None) -> TextAnnotation:
        if position is None:
            position = [2, 1, 2]
        color = Color(1, 0, 0, 1)
        main_link = self.latest_contact_points[0].link_a
        main_link.color = color
        for link in self.links_lost_contact():
            link.color = color
        text = f"{main_link.name} lost contact with {self.link_names_lost_contact()}"
        self.text_id = World.current_world.add_text(
            text,
            position,
            color=color)
        return TextAnnotation(text, position, color, self.text_id)

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
        self.text_id: Optional[int] = None

    def record_end_timestamp(self):
        self.end_timestamp = time.time()

    def duration(self):
        if self.end_timestamp is None:
            return None
        return self.end_timestamp - self.timestamp

    def annotate(self, position: Optional[List[float]] = None) -> TextAnnotation:
        if position is None:
            position = [2, 1, 2]
        color = Color(0, 1, 0, 1)
        self.hand.set_color(color)
        self.object.set_color(color)
        text = f"Picked {self.object.name}"
        self.text_id = World.current_world.add_text(text,
                                                    position,
                                                    color=color)
        return TextAnnotation(text, position, color, self.text_id)

    def __str__(self):
        return f"Pick up event: Hand:{self.hand.name}, Object: {self.object.name}, Timestamp: {self.timestamp}"

    def __repr__(self):
        return self.__str__()


class EventLogger:
    def __init__(self, annotate_events: bool = False):
        self.timeline = {}
        self.event_queue = queue.Queue()
        self.lock = threading.Lock()
        self.annotate_events = annotate_events
        if annotate_events:
            self.annotation_queue = queue.Queue()
            self.annotation_thread = EventAnnotationThread(self)
            self.annotation_thread.start()

    def log_event(self, thread_id, event: Event):
        self.event_queue.put((thread_id, event))
        if self.annotate_events:
            self.annotation_queue.put(event)
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
        if self.annotate_events:
            self.annotation_thread.stop()
            self.annotation_queue.join()
        self.event_queue.join()

    def __str__(self):
        return '\n'.join([' '.join([str(v) for v in values]) for values in self.get_events().values()])


class EventAnnotationThread(threading.Thread):
    def __init__(self, logger: EventLogger,
                 initial_z_offset: float = 2,
                 step_z_offset: float = 0.2,
                 max_annotations: int = 5):
        super().__init__()
        self.logger = logger
        self.initial_z_offset = initial_z_offset
        self.step_z_offset = step_z_offset
        self.current_annotations: List[TextAnnotation] = []
        self.max_annotations = max_annotations
        self.exit = False

    def get_next_z_offset(self):
        return self.initial_z_offset - self.step_z_offset * len(self.current_annotations)

    def run(self):
        while not self.exit:
            try:
                event = self.logger.annotation_queue.get(timeout=1)
            except queue.Empty:
                continue
            self.logger.annotation_queue.task_done()
            if len(self.current_annotations) >= self.max_annotations:
                # Move all annotations up and remove the oldest one
                for text_ann in self.current_annotations:
                    World.current_world.remove_text(text_ann.id)
                self.current_annotations.pop(0)
                for text_ann in self.current_annotations:
                    text_ann.position[2] += self.step_z_offset
                    text_ann.id = World.current_world.add_text(text_ann.text,
                                                               text_ann.position,
                                                               color=text_ann.color)
            z_offset = self.get_next_z_offset()
            text_ann = event.annotate([1.5, 1, z_offset])
            self.current_annotations.append(text_ann)
            time.sleep(0.1)

    def stop(self):
        self.exit = True
        self.join()
