import inspect
import logging
import sys

from requests import ConnectTimeout
from typing_extensions import List, Type, Dict, Optional
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin


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

    def get_class_with_name(self, class_name: str) -> Type:
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

    def get_class_names(self) -> List[str]:
        """
        Get all class names in a module
        :return: a list of class names
        """
        classes = self.get_all_classes()
        return [cls.__name__ for cls in classes]

    def get_all_classes(self) -> List[Type]:
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

    def get_all_classes_dict(self) -> Dict[str, Type]:
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


class RepositorySearch:
    """
    A class to search for similar file names within an online repository.
    """
    skip_folders: Optional[List[str]] = ['refills_models']
    stack: Optional[List[str]] = []

    def __init__(self, repository_url: str, timeout: Optional[int] = 0.04, start_search_in: Optional[List[str]] = None):
        """
        Initialize the RepositorySearch object with the repository URL.

        Parameters:
        - repository_url (str): The URL of the online repository.
        - timeout (int): The timeout for the HTTP requests.
        - start_search_in (list of str): The list of URLs to start the search in.
        """
        self.repository_url = repository_url
        self.timeout = timeout
        self.max_tries = 3
        self.all_file_links = set()
        self.all_file_names = set()
        if start_search_in is not None:
            self.stack.extend(start_search_in)

    def get_links_from_page(self, page_url: str) -> List[str]:
        """
        Retrieve all the links present on a webpage.

        Parameters:
        - page_url (str): The URL of the webpage.

        Returns:
        - list of str: List of links found on the webpage.
        """
        response = self.try_get_response(page_url)
        if response.status_code == 200:
            soup = BeautifulSoup(response.content, 'html.parser')
            links = [urljoin(page_url, link['href']) for link in soup.find_all('a', href=True)]
            links = [link for link in links if link.startswith(page_url) and link != page_url]
            self.all_file_links.update(links)  # Add links to the set
            self.all_file_names.update([link.split('/')[-1] for link in links])  # Extract file names
            return links
        else:
            logging.error(f"Failed to fetch content from {page_url}. Status code: {response.status_code}")
            return []

    def try_get_response(self, url: str) -> requests.Response:
        """
        Try to get the response from the URL. If the request fails, log the error.

        Parameters:
        - url (str): The URL to get the response from.
        """
        n_tries = 0
        timeout = self.timeout
        while n_tries <= self.max_tries:
            try:
                response = requests.get(url, timeout=timeout)
                return response
            except ConnectTimeout:
                logging.error(f"Request to {url} timed out.")
                timeout += 0.16
                response = None
            except Exception as e:
                logging.error(f"Failed to fetch content from {url}. Error: {e}")
                timeout += 0.16
                response = None
            if response is not None:
                break
            n_tries += 1

    def search_in_folder(self, folder_url: str, search_query: str) -> List[str]:
        """
        Recursively search for similar file names within a folder and its subfolders.

        Parameters:
        - folder_url (str): The URL of the folder to search in.
        - search_query (str): The query to search for in file names.

        Returns:
        - list of str: List of similar file names found within the folder.
        """
        folder_links = self.get_links_from_page(folder_url)
        similar_files_in_folder = []
        for link in folder_links:
            if link.endswith('/'):
                # It's a folder, recursively search in it
                similar_files_in_folder.extend(self.search_in_folder(link, search_query))
            elif search_query.lower() in link.lower():
                similar_files_in_folder.append(link)
        return similar_files_in_folder

    def search_similar_file_names(self, search_query: List[str], find_all: Optional[bool] = True,
                                  ignore: Optional[List[str]] = None) -> List[str]:
        """
        Search for similar file names within the repository.

        Parameters:
        - search_query (str): The query to search for in file names.
        - find_all (bool): If True, find all similar file names. If False, return the first match.
        - ignore (list of str): List of filename patterns to ignore.
        Returns:
        - list of str: List of similar file names found within the repository.
        """
        stack = [self.repository_url] + self.stack
        similar_files = []
        if ignore is None:
            ignore = []

        while stack:
            folder_url = stack.pop()
            folder_links = self.get_links_from_page(folder_url)
            for link in folder_links:
                if any(folder in link for folder in self.skip_folders):
                    continue
                if link.endswith('/'):
                    stack.append(link)
                else:
                    if any(query.lower() in link.lower() for query in search_query) and \
                            all(ig.lower() not in link.lower() for ig in ignore):
                        similar_files.append(link)
                        if not find_all:
                            return similar_files

        return similar_files

