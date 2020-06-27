from src import BaseCrawler
from bs4 import BeautifulSoup
import requests
from src.CrawlerEnums import BaseEnums
from uuid import uuid4
import json


class BassCrawlerImpl(BaseCrawler):
    def __init__(self):
        self._state_wise_serial_map = {}
        self._key_check_obj = None
        self._processed_states = []
        self._key_for_unique_check = None
        self.current_cluster = None
        self.current_district = None
        self.current_block = None
        self.current_state = None
        self._url = None
        self._response = None
        self.base_url = None
        self.first_end_point = None
        self.SCHOOLS_MAPPING = {}
        self.COLLECTIONS = {
            "STATE": None,
            "DISTRICT": None,
            "BLOCK": None,
            "CLUSTERS": []
        }

    @property
    def base(self):
        if not self._url:
            raise ValueError("Could not find a valid Url in Storage.")
        self._response = requests.get(self._url)
        if self._response.status_code != 200:
            print(f"{self._response.status_code} is received and {self._response.raw} is.")
            raise Exception("Something went wrong, please check the logs.")
        return self._response.text

    @base.setter
    def base(self, url):
        self._url = url

    def get_text_data(self, url):
        self.base = url
        soup = BeautifulSoup(self.base, 'html.parser')
        my_child = soup.find('table').children
        _table_texts = [table.text.strip().split('\n') for table in my_child if table != '\n']
        return _table_texts

    def get_links(self, base):
        """
        :return: list of further obtained endpoints
        """
        self.base = base
        soup = BeautifulSoup(self.base, 'html.parser')
        my_child = soup.find('table').find_all('a')
        _table_texts = [endpoint.get('href') for endpoint in my_child]
        return _table_texts

    def process_states(self, url):
        all_states = self.get_links(url)
        file_name = "states.txt"
        processed_states = open(file_name, 'r')
        for processed_state in processed_states:
            self._processed_states.append(processed_state[:-1])
        processed_states.close()
        while True:
            print("S.No.\t\tStates\n")
            print(self._processed_states)
            for i, state in enumerate(all_states):
                if state not in self._processed_states:
                    print(str(i) + "\t\t\t" + state)
                    self._state_wise_serial_map.setdefault(str(i), state)
            state = input(
                "What state you want to process, enter S.No. for single state, 'all' for ALL, 'exit' to EXIT: ")
            if state.upper() == 'ALL':
                for state in all_states:
                    if state not in self._processed_states:
                        print(self.base_url)
                        self.current_state = state
                        self.COLLECTIONS.__setitem__(BaseEnums.STATE.value, state)
                        next_url = self.base_url + BaseEnums.DELIMITER.value + state
                        print('State Next URL-> ', next_url)
                        self.process_districts(next_url)
                        self._processed_states.append(state)
                        state_file = open(file_name, 'a')
                        state_file.write(state + "\n")
                        state_file.close()
            elif state.upper() == 'EXIT':
                break
            else:
                s = self._state_wise_serial_map.get(state)
                self.current_state = s
                self.COLLECTIONS.__setitem__(BaseEnums.STATE.value, s)
                next_url = self.base_url + BaseEnums.DELIMITER.value + s
                print('State URL-> ', next_url)
                self.process_districts(next_url)
                self._processed_states.append(s)
                state_file = open(file_name, 'a')
                state_file.write(s + "\n")
                state_file.close()

    def process_districts(self, url):
        all_districts = self.get_links(url)
        for district in all_districts:
            self.current_district = district.rsplit('/', maxsplit=1)[1]
            self.COLLECTIONS.__setitem__(BaseEnums.DISTRICT.value, self.current_district)
            next_url = self.base_url + BaseEnums.DELIMITER.value + district
            self.process_blocks(next_url)

    def process_blocks(self, url):
        all_blocks = self.get_links(url)
        for block in all_blocks:
            self.current_block = block.rsplit('/', maxsplit=1)[1]
            self.COLLECTIONS.__setitem__(BaseEnums.BLOCK.value, self.current_block)
            next_url = self.base_url + BaseEnums.DELIMITER.value + block
            self.process_clusters(next_url)
            with open("state-wise-mappings.json", "a") as file:
                file.write(json.dumps(self.COLLECTIONS) + "\n")
                file.close()
            print(self.COLLECTIONS)

    def process_clusters(self, url):
        all_clusters = self.get_links(url)
        # Emptying the list so that block wise clusters can be stored on each execution
        self.COLLECTIONS.__setitem__(BaseEnums.CLUSTER.value, [])
        for cluster in all_clusters:
            self.current_cluster = cluster.rsplit('/', maxsplit=1)[1]
            self.COLLECTIONS.get(BaseEnums.CLUSTER.value).append(self.current_cluster)
            next_url = self.base_url + BaseEnums.DELIMITER.value + cluster
            self.process_school_entities(next_url)
            file_name = self.current_state + '.json'
            file = open(file_name, "a")
            file.write(json.dumps(self.SCHOOLS_MAPPING) + "\n")
            file.close()
            print(
                "\nDEBUG: CLUSTER: {1}, STATE: {2}, Total: {0}.\n".format(
                    len(
                        self.SCHOOLS_MAPPING
                    ), self.current_cluster, self.current_state
                )
            )
            self.SCHOOLS_MAPPING = {}

    def process_school_entities(self, url):
        all_school_links = self.get_links(url)
        data_object = {}
        for school in all_school_links:
            school_url = self.base_url + BaseEnums.DELIMITER.value + school
            data_object.setdefault('endpoint', school)
            data_object.setdefault('url', school_url)
            data_object.setdefault('cluster', self.current_cluster)
            data_object.setdefault('block', self.current_block)
            data_object.setdefault('district', self.current_district)
            data_object.setdefault('state', self.current_state)
            self.check_if_key_exists = [self.generate_unique_id, data_object]
            self.SCHOOLS_MAPPING.setdefault(self.check_if_key_exists, data_object)

    @property
    def check_if_key_exists(self):
        if self._key_for_unique_check in self._key_check_obj.keys():
            self.check_if_key_exists = [self.generate_unique_id, self._key_check_obj]
            return self.check_if_key_exists
        return self._key_for_unique_check

    @check_if_key_exists.setter
    def check_if_key_exists(self, key_obj_pair: list):
        self._key_for_unique_check, self._key_check_obj = key_obj_pair[0], key_obj_pair[1]

    @property
    def generate_unique_id(self):
        unique_id = uuid4().__str__()
        return unique_id

    def begin_execution(self, url):
        x = url.rsplit('/', maxsplit=1)
        print('x', x)
        self.base_url = x[0]
        print(self.base_url)
        self.process_states(url)
