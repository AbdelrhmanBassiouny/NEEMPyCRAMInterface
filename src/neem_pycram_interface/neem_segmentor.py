from pycram.datastructures.enums import WorldMode
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
        obj_gen = self.pni.yield_motions_in_query()
        hand_obj = None
        for obj in obj_gen:
            if hand_obj is None:
                hand_obj = [obj for obj in self.world.objects if "hand" in obj.name.lower()][0]
            contact_points = hand_obj.closest_points(0.03)
            for contact_point in contact_points:
                print(f"Contact point: {contact_point}")
                obj_in_contact = self.world.get_object_by_id(contact_point[2])
                print(f"Hand is in contact with {obj_in_contact.name}")
                print(f"Object position: {obj_in_contact.get_position()}")
                # Track the object in contact, and compare it's pose with the pose of the hand.
                # If the object is moved by the hand, the pose of the object will change.
                # and the object should be no more in contact with the support surface.
                # consult the ontology to see what triples are needed to describe the situation before,
                # during and after the contact.
