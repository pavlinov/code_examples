from app.config.shared import *
from app.common import queries as common_queries
from app.common.caboose_queries.collectibles import UnopenableDefinition, CollectibleInstance
from app.common.caboose_queries.users import Profile
from app.caboose_queries.cms import FindOwnerLogs
from bson.objectid import ObjectId
from uuid import UUID
import copy
import datetime
import threading
import logging

IDS_IN_REQUEST = 3
PAGE_COUNT = 25000

SHOW_COUNT = 10

QUERIES_PER_USER = 5

class DeferredQueryFindOwners(object):
    DRAFT = 'draft'
    PENDING = 'pending'
    COMPLETED = 'completed'
    FAILED = 'failed'

    STATUSES = (COMPLETED, PENDING, FAILED)
    TABS = zip(STATUSES, ('Completed', 'In Progress', 'Failed'))
    CONTEXTUAL_STATES = dict(zip(STATUSES, ('success', 'warning', 'error')))  # has-* :.control-label .form-control .help-block
    CONTEXTUAL_CLASSES = dict(zip(STATUSES, ('success', 'warning', 'danger')))  # alert-*, div.*>

    DEFAULT_FIELDS = dict(
        status=DRAFT,
        name='',
        collectible_ids=[],
        message='',
        results={},
        owned_at=None,
        processed_collectibles_count=0,
        percentage=0
    )

    @staticmethod
    def get_query_by_id(query_id):
        return FindOwnerLogs.find_one({'id': query_id})

    @staticmethod
    def get_queries_by_status(status):
        queries = FindOwnerLogs.find(
            {'status': status},
            select_fields=['name', 'collectible_ids', 'collectibles_map', 'status', 'message', 'owned_at', 'created_at', 'cms_user_id'],
            sort_fields=[{'created_at': -1}]
        )
        return queries

    @staticmethod
    def get_collectibles_map(valid_ids):
        collectibles_map = {collectible['id']: collectible['internal_name'] for collectible in UnopenableDefinition.find({'id': list(set(valid_ids))}, select_fields=['internal_name'])}
        return collectibles_map

    @staticmethod
    def get_profiles_map(ids):
        profiles_map = {}
        
        for batch_profile_ids in [ids[x:x+1000] for x in xrange(0, len(ids), 1000)]:
            profiles_map.update({profile['id']: profile['user']['user_name'] for profile in Profile.find({'id': list(set(batch_profile_ids))}, select_fields=[{"user": ["user_name"]}])})
        
        return profiles_map

    @staticmethod
    def statuses():
        return DeferredQueryFindOwners.STATUSES

    @staticmethod
    def tabs():
        return DeferredQueryFindOwners.TABS

    @staticmethod
    def get_contextual_states():
        return DeferredQueryFindOwners.CONTEXTUAL_STATES

    @staticmethod
    def get_contextual_classes():
        return DeferredQueryFindOwners.CONTEXTUAL_CLASSES

    @staticmethod
    def show_count():
        return SHOW_COUNT

    @staticmethod
    def create(parameters):
        result = FindOwnerLogs.create([parameters])
        return result

    @staticmethod
    def update(query_id, parameters):
        return FindOwnerLogs.update({'id': query_id}, parameters)

    @staticmethod
    def find_log(query_id):
        return FindOwnerLogs.find_one({'id': query_id})

    @staticmethod
    def new():
        fields = copy.deepcopy(DeferredQueryFindOwners.DEFAULT_FIELDS)
        return fields

    @staticmethod
    def validate_ids(ids):
        valid_ids = []
        invalid_ids = []
        for id in ids:
            try:
                UUID(id, version=4)
                valid_ids.append(id)
            except ValueError:
                invalid_ids.append(id)

        return valid_ids, invalid_ids

    @staticmethod
    def get_deferred_query_param(application):
        fields = copy.deepcopy(DeferredQueryFindOwners.DEFAULT_FIELDS)

        card_ids = application.get_argument('card_ids', '')
        fields['collectible_ids'] = card_ids.replace(' ', '').split(',')
        collectibles_count = len(fields['collectible_ids'])
        fields['name'] = '{} card query by {}'.format(collectibles_count, application.current_user['name'])
        if application.get_argument('query_name'):
            fields['name'] = application.get_argument('query_name')
        fields['cms_user_id'] = application.current_user['id']
        fields['owned_at'] = datetime.datetime.utcnow()
        if application.get_argument('owned_at'):
            fields['owned_at'] = common_queries.tz_to_utc(common_queries.date_string_to_datetime(application.get_argument('owned_at')), tzone=application.current_user['timezone'])

        return fields

    @staticmethod
    def write_query(query, query_id=None):
        if not query_id:
            new_queries = DeferredQueryFindOwners.create(query)
            query_id = new_queries[0]['id']
        else:
            DeferredQueryFindOwners.update(query_id, query)
        return query_id

    @staticmethod
    def make_query(application, query, query_id):
        valid_ids = query.get('collectible_ids')
        owned_at = query.get('owned_at')
        try:
            results = DeferredQueryFindOwners.find_owners(valid_ids, owned_at, query_id, application)
        except Exception as ex:
            DeferredQueryFindOwners.update(query_id, {'message': ex.message, 'status': DeferredQueryFindOwners.FAILED})
            return False

        message = 'Query successfully completed, owners found: {}'.format(len(results.get('intersect_results')))
        DeferredQueryFindOwners.update(query_id, {'results': results, 'status': DeferredQueryFindOwners.COMPLETED, 'message': message})

        return True

    @staticmethod
    def run_findowners_thread(application, query, query_id=None):
        new_thread_name = str(application.current_user['id'])
        threads_running = [thread for thread in threading.enumerate() if thread.name == new_thread_name]
        if (len(threads_running) == QUERIES_PER_USER):
                raise Exception('Query limit reached. Please wait while at least one of your previous query will finished.')

        query['status'] = DeferredQueryFindOwners.PENDING
        query_id = DeferredQueryFindOwners.write_query(query, query_id)

        background = threading.Thread(target=DeferredQueryFindOwners.make_query, args=(application, query, query_id))
        background.setName(new_thread_name)
        background.setDaemon(True)
        background.start()
        return query_id

    @staticmethod
    def find_owners_by_date(ids, owned_at, query_id, application):
        results = {}

        start_time = common_queries.date_string_to_datetime(owned_at)
        end_time = start_time + datetime.timedelta(seconds=0)
        now = datetime.datetime.utcnow()

        DeferredQueryFindOwners.update(query_id, {'message': 'Starting process', 'percentage': 0})
        for batch_card_ids in [ids[x:x+IDS_IN_REQUEST] for x in xrange(0, len(ids), IDS_IN_REQUEST)]:
            log = DeferredQueryFindOwners.find_log(query_id)
            cards_count = len(batch_card_ids)
            processed_percentage = float('{:2.0f}'.format(log['processed_collectibles_count'] * 100 / len(log['collectible_ids'])))
            DeferredQueryFindOwners.update(query_id, {'message': 'Getting {} card(s)'.format(cards_count), 'percentage': processed_percentage})

            where = {'collectible_definition_id': list(batch_card_ids), 'created_at': {'le': owned_at}, 'owner_type': 'profile'}
            select_fields = ["collectible_definition_id", "owner_id", "owner_type", "history"]
            sorts = [{'id': 1}]
            instance_page = 0
            previous_items_count = PAGE_COUNT
            while True:
                instance_page += 1
                if PAGE_COUNT > previous_items_count:
                    break

                collectible_instances = CollectibleInstance.find(where, sort_fields=sorts, page=instance_page, page_count=PAGE_COUNT, select_fields=select_fields)
                processed_percentage = float('{:2.0f}'.format((log['processed_collectibles_count'] + 1) * 100 / len(log['collectible_ids'])))
                DeferredQueryFindOwners.update(query_id, {'message': 'Processing {} card(s)'.format(cards_count), 'percentage': processed_percentage})

                previous_items_count = len(collectible_instances)
                if not collectible_instances:
                    break

                for card_instance in collectible_instances:
                    for j, owner_history in enumerate(card_instance['history']):
                        owner_start = common_queries.date_string_to_datetime(owner_history['timestamp']).replace(tzinfo=None)
                        owner_end = common_queries.date_string_to_datetime(card_instance['history'][j + 1]['timestamp']).replace(tzinfo=None) if j + 1 < len( card_instance['history']) else now
                        if ((start_time <= owner_start <= end_time) or (owner_start <= start_time <= owner_end)) and owner_history['owner_type'] == 'profile':
                            if owner_history.get('owner_id') in results:
                                if card_instance['collectible_definition_id'] in results[owner_history['owner_id']]:
                                    results[owner_history['owner_id']][card_instance['collectible_definition_id']] += 1
                                else:
                                    results[owner_history['owner_id']][card_instance['collectible_definition_id']] = 1
                            else:
                                results[owner_history.get('owner_id')] = {card_instance['collectible_definition_id']: 1}
            DeferredQueryFindOwners.update(query_id, {'message': 'Processed {} card(s)'.format(cards_count), 'processed_collectibles_count': log['processed_collectibles_count'] + cards_count, 'percentage': processed_percentage})

        DeferredQueryFindOwners.update(query_id, {'message': 'Finished process', 'percentage': 100})
        return results

    @staticmethod
    def find_owners(valid_ids, owned_at, query_id, application):
        owner_results = DeferredQueryFindOwners.find_owners_by_date(valid_ids, owned_at, query_id, application)
        requested_cards_count = len(valid_ids)
        card_instances_count = 0
        intersect_results = []
        owners = []
        for owner, cards in owner_results.iteritems():
            if len(cards) == requested_cards_count:
                card_instances_count += sum(cards.values())
                intersect_results.append({owner: cards})
                owners.append(owner)

        profiles_map = DeferredQueryFindOwners.get_profiles_map(owners)

        result = {
            'requested_cards_count': requested_cards_count,
            'card_instances_count': card_instances_count,
            'intersect_results': intersect_results,
            'profiles_map': profiles_map,
        }
        return result
