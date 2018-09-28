""" Routines to manage firebase database entries for stories, locations,
text.

Class DBItem: An element (item) of a firebase database.

    Attributes:
        category: firebase category (see GL_KNOWN_CATEGORIES)
        idx: index within the category
        record: dict holding data for the item

Class DB: Firebase database, with methods for manipulating
    DBItem instances, derived from the firebase.FirebaseApplication class.  

    Includes inherited methods put, post, get, delete
     (ref https://ozgur.github.io/python-firebase/)

"""

import re

import datetime
from dateutil.parser import parse
from firebase.firebase import FirebaseApplication

FIREBASE_URL = 'https://overview-seeds.firebaseio.com'
FIREBASE_GL_URL = 'https://good-locations.firebaseio.com'
FIREBASE_NEG_URL = 'https://negative-training-cases.firebaseio.com/'

FB_FORBIDDEN_CHARS = u'[.$\%\[\]#/?\n]'
BASE_CATEGORY = '/stories'

# Firebase deletes keys with empty dicts as values.  For classification,
# we need the empty data record.
EMPTY_DATA_VALUES = {
    'description': '',
    'image': '',
    'image_tags': {},
    'keywords': {},
    'locations': {},
    'outlet': '',
    'probability': 0.,
    'publication_date': '',
    'text': '',
    'title': '',
    'url': ''
}

# For server-side date filtering.  Database Rules must include
# (e.g.. for top-level key 'stories'):
# "stories": {".indexOn": ["publication_date"]}
EPOCH_START = '1970-01-01'
NEXT_CENTURY = '2100-01-01'


class DB(FirebaseApplication):
    """Firebase database. 

    Attributes:
        url: location of the firebase database

    Methods for manipulating DBItem instances:
        put_item
        check_known (if item is in database)
        grab_data (materials from specified subheading)
        grab_stories (all stories from a category)
        delete_item
        delete_category
        delete_all_mentions (of item from specified categories)
        
    """

    def __init__(self, database_url):
        FirebaseApplication.__init__(self, database_url, None)
        self.url = database_url
    
    def put_item(self, item):
        """
        Upload an item to databse. Returns the record if successful,
        otherwise None.
        """
        return self.put(item.category, item.idx, item.record)

    def check_known(self,item):
        if self.get(item.category, item.idx) is None:
            return False
        else:
            return True

    def grab_data(self,
                  category=BASE_CATEGORY,
                  startDate=EPOCH_START,
                  endDate=NEXT_CENTURY,
                  data_type='text'):
        """Download specified materials for specified dates.

        Arguments:
            category: database top-level key
            startDate/endDate: isoformat date or datetime 
            data_type: can be 'text', 'keywords', 'image', or any
                secondary heading listed in EMPTY_DATA_VALUES.

        Returns:  List of story indices and list of data.
        """
        params = {
            'orderBy': '"publication_date"',
            'startAt': '"' + startDate + '"',
            'endAt': '"' + endDate + '"'
        }
        raw = self.get(category, None, params=params)
        indices = list(raw.keys())
        data = []
        for v in raw.values():
            try: 
                data.append(v[data_type])
            except KeyError:
                try:
                    data.append(EMPTY_DATA_VALUES[data_type])
                except KeyError:
                    print('Firebaseio: No EMPTY_DATA_VALUE assigned.\n')
                    raise
        return indices, data

    def grab_stories(self,
                     category=BASE_CATEGORY,
                     startDate=EPOCH_START,
                     endDate=NEXT_CENTURY):
        """Download all stories in a given category between given dates.

        Arguments:
            category: database top-level key
            startDate/endDate: isoformat date or datetime 

        Returns a list of DBItems.
        """
        params = {
            'orderBy': '"publication_date"',
            'startAt': '"' + startDate + '"',
            'endAt': '"' + endDate + '"'
        }
        raw = self.get(category, None, params=params)
        stories = [DBItem(category, idx, record) for idx, record in
                   raw.items()]
        return stories
                
    def delete_item(self,item):
        self.delete(item.category, item.idx)
        return

    def delete_category(self, category):
        self.delete(category, None)
        return

    def delete_all_mentions(self, idx, categories=[BASE_CATEGORY]):
        for c in categories:
            self.delete(c, idx)
        return

class DBItem(object):
    """Creates a firebase database item.

    Attributes:
        category: firebase category (see GL_KNOWN_CATEGORIES)
        idx: index within the category
        record: dict holding data for the item

    """

    def __init__(self, category, idx=None, record=None):

        if idx is None and record is None:
            raise ValueError
        self.category = category
        self.record = record
        if idx is None:
            self.idx = self.make_idx()
        else:
            self.idx = idx

    def make_idx(self, max_len=96):
        """Construct an index for a database item.

        Generically, idx is based on title. Lacking title, the url is
        substituted.

        Returns: a unicode string.
        """
        try:
            idx = self.record['title']
        except KeyError:
            try:
                idx = self.record['url']
            except KeyError:               
                raise
        idx = re.sub(FB_FORBIDDEN_CHARS,'',idx)
        return idx[:max_len]
    
