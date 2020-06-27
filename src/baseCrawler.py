from abc import ABC, abstractmethod


class BaseCrawler(ABC):
    @abstractmethod
    def base(self):
        pass

    @abstractmethod
    def process_states(self, url):
        pass

    @abstractmethod
    def process_districts(self, url):
        pass

    @abstractmethod
    def process_blocks(self, url):
        pass

    @abstractmethod
    def process_clusters(self, url):
        pass

    @abstractmethod
    def process_school_entities(self, url):
        pass
