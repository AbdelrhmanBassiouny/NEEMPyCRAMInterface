import inspect
import logging
import sys


class ModuleInspector:
    """
    A class to inspect a module.
    """

    def __init__(self, module_name: str):
        self.module_name = module_name

    def print_classes(self) -> None:
        """
        Print all class names in a module
        """
        class_names = self.get_class_names()
        for class_name in class_names:
            print(class_name)

    def get_class_with_name(self, class_name: str) -> type:
        """
        Get the class with a specific name in a module
        :param class_name: the name of the class.
        :return: the class with the given name
        """
        classes = self.get_all_classes()
        for cls in classes:
            if cls.__name__ == class_name:
                return cls
        logging.error(f"Class {class_name} not found in module {self.module_name}")
        raise ValueError(f"Class {class_name} not found in module {self.module_name}")

    def get_class_names(self) -> list:
        """
        Get all class names in a module
        :return: a list of class names
        """
        classes = self.get_all_classes()
        return [cls.__name__ for cls in classes]

    def get_all_classes(self) -> list:
        """
        Get all class names in a module
        :return: a list of classes
        """
        classes = []
        for name, obj in inspect.getmembers(sys.modules[self.module_name]):
            if inspect.isclass(obj):
                if self.module_name in obj.__module__:
                    classes.append(obj)
        return classes

    def get_all_classes_dict(self) -> dict:
        """
        Get all class names in a module
        :return: a list of classes
        """
        classes = {}
        for name, obj in inspect.getmembers(sys.modules[self.module_name]):
            if inspect.isclass(obj):
                if self.module_name in obj.__module__:
                    classes[obj.__name__] = obj
        return classes
