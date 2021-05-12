from app.config.settings import *
from app.config.shared import *
from app.common.config.shared import *
from app.common import queries as common_queries
from app.caboose_queries.cms import FindOwnerLogs
from app.services.deferred_query_findowners import DeferredQueryFindOwners
from brubeck.auth import authenticated, web_authenticated
from brubeck.templating import Jinja2Rendering
from app.handlers import BaseHandler, JSONBaseHandler, DownloadBaseHandler
from brubeck.datamosh import StreamedHandlerMixin

class DeferredQueriesList(BaseHandler, Jinja2Rendering):
    @web_authenticated
    def get(self):
        current_tab = self.get_argument('tab', 'completed')
        queries = DeferredQueryFindOwners.get_queries_by_status(current_tab)
        
        for query in queries:
            query['owned_at'] = common_queries.utc_to_tz(query['owned_at'], tzone=self.current_user['timezone']).strftime("%a, %b %d, %Y %I:%M %p")
        
        context = {
            'user': self.current_user,
            'current_tab': current_tab,
            'queries': queries,
            'show_count': DeferredQueryFindOwners.show_count(),
            'tabs': DeferredQueryFindOwners.tabs(),
            'CONTEXTUAL_CLASSES': DeferredQueryFindOwners.get_contextual_classes(),
        }
        return self.render_template('findowners/list.html', **context)

class DeferredQueriesCreate(BaseHandler, Jinja2Rendering):
    @web_authenticated
    def get(self):
        query = DeferredQueryFindOwners.new()

        flash = self.get_cookie('flash')
        self.delete_cookie('flash', path='/')
        flash_error = self.get_cookie('flash_error')
        self.delete_cookie('flash_error', path='/')

        context = {
            'action': 'new',
            'user': self.current_user,
            'flash': flash,
            'query': query,
            'CONTEXTUAL_STATES': DeferredQueryFindOwners.get_contextual_states(),
            'CONTEXTUAL_CLASSES': DeferredQueryFindOwners.get_contextual_classes(),
        }
        return self.render_template('findowners/edit.html', **context)

    @web_authenticated
    def post(self):
        query = DeferredQueryFindOwners.get_deferred_query_param(self)
        query['owned_at'] = query['owned_at'].strftime('%Y-%m-%d %H:%M:%S.%f')
        valid_ids, invalid_ids = DeferredQueryFindOwners.validate_ids(list(set(query.get('collectible_ids'))))
        query['invalid_collectible_ids'] = invalid_ids
        query['collectibles_map'] = DeferredQueryFindOwners.get_collectibles_map(valid_ids)
        new_query_id = None
        try:
            if invalid_ids:
                raise Exception('Invalid ids: {}'.format(', '.join(invalid_ids)))
            query['collectible_ids'] = valid_ids

            new_query_id = DeferredQueryFindOwners.run_findowners_thread(self, query)
        except Exception as ex:
            query['status'] = DeferredQueryFindOwners.FAILED
            query['message'] = ex.message
            new_query_id = DeferredQueryFindOwners.write_query(query, new_query_id)
            self.set_cookie('flash_error', "Find owners query creation failed: {}".format(ex.message), path='/')
            return self.redirect('/%s/cards/deferred_queries/owners/edit/%s' % (APP_URL, new_query_id))


        self.set_cookie('flash', 'New query created. Wait while query will be processed', path='/')
        return self.redirect('/%s/cards/deferred_queries/owners/edit/%s' % (APP_URL, new_query_id))

class DeferredQueriesEdit(BaseHandler, Jinja2Rendering):
    @web_authenticated
    def get(self, query_id):
        query = DeferredQueryFindOwners.get_query_by_id(query_id)
        if not query:
            return self.render_error('404 - unable to find query in db. id = %s' % query_id)
            
        if query['owned_at']:
            query['owned_at'] = common_queries.utc_to_tz(query['owned_at'], tzone=self.current_user['timezone']).strftime("%a, %b %d, %Y %I:%M %p")

        flash = self.get_cookie('flash')
        self.delete_cookie('flash', path='/')
        flash_error = self.get_cookie('flash_error')
        self.delete_cookie('flash_error', path='/')

        context = {
            'action': 'edit',
            'user': self.current_user,
            'flash': flash,
            'flash_error': flash_error,
            'query': dict(query),
            'CONTEXTUAL_STATES': DeferredQueryFindOwners.get_contextual_states(),
            'CONTEXTUAL_CLASSES': DeferredQueryFindOwners.get_contextual_classes(),
        }
        return self.render_template('findowners/edit.html', **context)

    @web_authenticated
    def post(self, query_id):
        query = DeferredQueryFindOwners.get_deferred_query_param(self)
        query['owned_at'] = query['owned_at'].strftime('%Y-%m-%d %H:%M:%S.%f')
        valid_ids, invalid_ids = DeferredQueryFindOwners.validate_ids(list(set(query.get('collectible_ids'))))
        query['invalid_collectible_ids'] = invalid_ids
        query['collectibles_map'] = DeferredQueryFindOwners.get_collectibles_map(valid_ids)
        try:
            if invalid_ids:
                raise Exception('Invalid ids: {}'.format(', '.join(invalid_ids)))
            query['collectible_ids'] = valid_ids

            DeferredQueryFindOwners.run_findowners_thread(self, query, query_id)
        except Exception as ex:
            query['status'] = DeferredQueryFindOwners.FAILED
            query['message'] = ex.message
            DeferredQueryFindOwners.write_query(query, query_id)
            self.set_cookie('flash_error', "Find owners query creation failed: {}".format(ex.message), path='/')
            return self.redirect('/%s/cards/deferred_queries/owners/edit/%s' % (APP_URL, query_id))

        self.set_cookie('flash', 'Query saved. Wait while request will be processed', path='/')
        return self.redirect('/%s/cards/deferred_queries/owners/edit/%s' % (APP_URL, query_id))

class DeferredQueriesStatus(JSONBaseHandler, StreamedHandlerMixin):
    @authenticated
    def get(self, query_id):
        log = DeferredQueryFindOwners.find_log(query_id)
        if not log:
            log = {}
        self.add_to_payload('log', log)
        return self.render(status_code=200)


class DownloadCSV(DownloadBaseHandler):
    def get(self , query_id):
        type_export = self.get_argument('type_export')
        query = DeferredQueryFindOwners.get_query_by_id(query_id)
        body  = None
        not_empty = query.get('results' , {}).get('intersect_results' , {})
        if type_export == 'names_cards' and not_empty:
            body = 'Owner,Card,count\n'
            for p in query['results']['intersect_results']:
                row  = ''
                for owner , cards in p.iteritems():
                    for obj_id, num in cards.iteritems():
                        owner_to_file, card_to_file = '', ''
                        if query['results']['profiles_map']:
                            owner_to_file = query['results']['profiles_map'].get(owner, '')
                        if query['collectibles_map']:
                            card_to_file = query['collectibles_map'].get(obj_id , '')
                        row += '%s,%s,%s\n' % (owner_to_file , card_to_file, num)
                body += row
        elif type_export == 'owner_names' and not_empty:
            body  = 'Owner\n'
            for p in query['results']['intersect_results']:
                row  = ''
                for owner , cards in p.iteritems():
                    if query['results'].get('profiles_map'):
                        row += '%s\n' % (query['results']['profiles_map'].get(owner , ''))
                body += row
        if body:
            self.set_body(body)
            self.set_file_name('%s_owners_%d.csv' % (APP_NAME, time.time()))
        return self.render(status_code=200)