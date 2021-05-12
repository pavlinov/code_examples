import json, os
import datetime
import humanize
from config import Config as LambdaConfig
from CabooseUtilities.http.IPC import IPC
from CabooseUtilities.aws import sqs_client
from CabooseUtilities.aws import redis_client
from ToppsHttp.auth import ToppsAuthentication
SQS_NAME = LambdaConfig.config.SQS_NAME
REDIS_KEY = LambdaConfig.config.tag + '_last_run'
REDIS_CNX = redis_client.RedisConnectionInfo(host=LambdaConfig.config.REDIS_CACHE_HOST,
                                                      name=LambdaConfig.config.REDIS_CACHE_NAME,
                                                      port=LambdaConfig.config.REDIS_CACHE_PORT,
                                                      db_num=LambdaConfig.config.REDIS_CACHE_DB_NUM)
redis_client.connect(REDIS_CNX)

def lambda_handler(event, context):
    return_dict = {}
    print(f"running ContestWhoToScore with event: {event}")

    LambdaConfig.config['app'] = event.get("app", os.environ.get('app', LambdaConfig.config.app))
    LambdaConfig.config['request_id'] = event.get('request_id', ToppsAuthentication.generateRequestID())
    LambdaConfig.config['contests_url'] = event.get("contests_url", LambdaConfig.config.contests_url)
    '''
    1. Query the game_scores table to get all the game_scores entries that have been updated since the last time this lambda ran.
    2. Store off in Redis the greatest timestamp from any received game_scores retrieved in step one.
    3. This gives a list of game_keys. Turn this list (with possible duplicates) into a set
    4. Using this set of game_keys, build a list of contest_instances that have these game_keys as a part of it
    5. Using the list of contest_definitions, build a list of contest_intsances that have those definitions
    6. The output of this will be a list of affected contest_instances that have new scoring events.
    7. pump this list onto the ContestScoringSQS queue
    '''
    print(f"full config: {LambdaConfig.config}")
    right_now = datetime.datetime.utcnow()
    print(f"Now = {right_now}")
    last_run = redis_client.get_key(connection_name=LambdaConfig.config.REDIS_CACHE_NAME, key=REDIS_KEY)
    print(f"Last run in redis = {last_run}")
    #last_run = run_at - datetime.timedelta(days=LambdaConfig.config.MAX_INTERVAL_DAYS)
    if last_run:
        last_run = datetime.datetime.strptime(last_run, "%Y-%m-%d %H:%M:%S.%f")
    else:
        last_run = right_now - datetime.timedelta(days=LambdaConfig.config.MAX_INTERVAL_DAYS)
    print(f"Last processing acording to Redis: {humanize.naturaltime(last_run)}")
    games_scores = get_games_scores_by_modified_at(last_run, app=LambdaConfig.config.app, request_id=LambdaConfig.config.request_id)
    print(f"Received {len(games_scores)} game_scores from Contest API")
    game_ids = list()
    for score in games_scores:
        game_ids.append(score.get('game_id'))
        modified_at = datetime.datetime.strptime(score.get('modified_at'), "%Y-%m-%d %H:%M:%S.%f")
        if last_run < modified_at:
            last_run = modified_at
    game_ids = list(set(game_ids))
    print(f"There are {len(game_ids)} game ids with new scores")
    if len(game_ids) == 0:
        return_dict['Result'] = "No new Events for any of the games .. Nothing to do..."
        set_redis_run_time(last_run)
        return return_dict
    
    contest_definitions_response = get_contest_definitions_by_game_ids(game_ids, app=LambdaConfig.config.app, request_id=LambdaConfig.config.request_id)
    
    contest_ids = list()
    for contest in contest_definitions_response:
        contest_ids.append(contest.get('id'))
    contest_ids = list(set(contest_ids))
    print(f"Found {len(contest_definitions_response)} Contest Definitions that have one of the games in them...")

    if len(contest_ids) == 0:
        return_dict['num_game_scores'] = len(games_scores)
        return_dict['num_games'] = len(game_ids)
        return_dict['Result'] = "No Contest Definitions for those game scores and games."
        set_redis_run_time(last_run)
        return return_dict

    contest_instances_response = get_contest_instances_by_definition_ids(contest_ids, app=LambdaConfig.config.app , request_id=LambdaConfig.config.request_id)
    print(f"Found {len(contest_instances_response)} Contest Instances for the definitions ")

    if len(contest_instances_response) == 0:
        return_dict['Result'] = "Event though there are events, and games, and contest definitions, there are ZERO instances to score."
        set_redis_run_time(last_run)
        return return_dict

    sqs_message_instances_ids = list()
    for c_instance in contest_instances_response:
        sqs_item = dict()
        is_final = c_instance['scores'].get('is_final', False)
        sqs_item[c_instance.get('id')] = is_final
        sqs_message_instances_ids.append(sqs_item)
    
    
    print(f"Created {len(sqs_message_instances_ids)} entries for the SQS Queue")
    success, error_string = send_instances_to_scoring(sqs_message_instances_ids)
    if not success:
        return_dict['sqs_queue_name'] = SQS_NAME
        return_dict['error'] = error_string
        return_dict['Result'] = "BIGTIME Error pushing to the sqs queue?"
        return return_dict
    set_redis_run_time(last_run)
    return_dict['Result'] = f"successfully pushed {len(sqs_message_instances_ids)} instances out to be scored."
    return return_dict

def break_list(the_list, size):
    return [the_list[i * size:(i + 1) * size] for i in range((len(the_list) + size - 1) // size )]  

def send_instances_to_scoring(instance_dicts:list):
    final_batches = break_list(instance_dicts, 10)
    if final_batches:
        for batch in final_batches:
            success, error_string = sqs_client.push_batch(SQS_NAME, batch)
            if not success:
                return False, error_string
    return True, None

def set_redis_run_time(last_run_time):
    print(f"Setting 'last run' to {humanize.naturaltime(last_run_time)}")
    print(f"setting key: {REDIS_KEY} to value: {str(last_run_time)}")
    redis_client.set_key(connection_name=LambdaConfig.config.REDIS_CACHE_NAME, key=REDIS_KEY, value=str(last_run_time))

def process_response(obj, response):
    if response and response.success:
        body = response.body
        if body.get('data', None):
            return body.get('data').get(obj, [])
    raise Exception(response.body.error.reason)


def get_games_scores_by_modified_at(modified_at:str, app:str, request_id:str):
    if not modified_at:
        return []
    print(f"trying to find game_scores with a modified at time greater than: {humanize.naturaltime(modified_at)}")
    games_scores_request = {
        "select_fields": ["id", "game_id", "modified_at"],
        "filter_fields": {'modified_at': {'gt': str(modified_at)}},
        "page_count": LambdaConfig.config.PAGE_COUNT_LIMIT
    }
    games_scores_result = IPC(
        sender=LambdaConfig.config.tag, app=app, jwt=LambdaConfig.config.CABOOSE_JWT,
        host=LambdaConfig.config.CONTESTS_URL, endpoint=LambdaConfig.config.FIND_GAME_SCORES,
        body=games_scores_request,
        read_timeout=10, request_id=request_id, do_async=False
    )
    return process_response('game_scores', games_scores_result)


def get_contest_definitions_by_game_ids(game_ids:list, app:str, request_id:str):
    if not game_ids:
        return []
    contest_definitions_request = {
        "select_fields": ["id"],
        "filter_fields": {'game_ids': {"has_any": game_ids}},
        "page_count": LambdaConfig.config.PAGE_COUNT_LIMIT
    }
    contest_definitions_response = IPC(
        sender=LambdaConfig.config.tag, app=app, jwt=LambdaConfig.config.CABOOSE_JWT,
        host=LambdaConfig.config.CONTESTS_URL, endpoint=LambdaConfig.config.FIND_CONTEST_DEFINITIONS,
        body=contest_definitions_request,
        read_timeout=10, request_id=request_id, do_async=False
    )
    return process_response('contest_definitions', contest_definitions_response)

def get_contest_instances_by_definition_ids(contest_instances_ids:list, app:str, request_id:str):
    if not contest_instances_ids:
        return []
    contest_instances_request = {
        "select_fields": ["id", "scores"],
        "filter_fields": {'contest_definition_id': contest_instances_ids},
        "page_count": LambdaConfig.config.PAGE_COUNT_LIMIT
    }
    contest_instances_response = IPC(
        sender=LambdaConfig.config.tag, app=app, jwt=LambdaConfig.config.CABOOSE_JWT,
        host=LambdaConfig.config.CONTESTS_URL, endpoint=LambdaConfig.config.FIND_CONTEST_INSTANCES,
        body=contest_instances_request,
        read_timeout=10, request_id=request_id, do_async=False
    )
    return process_response('contest_instances', contest_instances_response)


# With the following, you can run this script locally before deploying to Lambda using test data.
if __name__ == "__main__":
    print("Running Locally using test data")

    # Load Test Data
    import pprint
    test_event_file = open("test_data.json")
    test_event_dict = json.loads(test_event_file.read())
    print("\nTest Data for Input:\n")
    pprint.pprint(test_event_dict)

    # Run the Lambda Handler
    print("\nLambda Starting\n")
    results = lambda_handler(test_event_dict, None)
    print("\nLambda Complete - Output:")
    pprint.pprint(results)
