import collections
import datetime
import json
import os
import time
import uuid

import boto3
import xmltodict
from time import sleep
from typing import Dict, Tuple, Sequence
from tqdm import tqdm

from google.cloud import datastore

from config import QUESTION_XML, MTURK_ENVIRONMENTS, MTURK_PROFILE_NAME, DEFAULT_TASK_QUALIFICATIONS, us_high_school_qualification, \
    MTURK_REGION_NAME, HOUR, mandatory_hit_attributes
from abc import ABC, abstractmethod
from utils import filter_non_ascii


def extract_free_text(answer_dict):
    if type(answer_dict['QuestionFormAnswers']['Answer']) is list:
        return {item['QuestionIdentifier']: item['FreeText'] for item in answer_dict['QuestionFormAnswers']['Answer']}
    else:
        r = answer_dict['QuestionFormAnswers']['Answer']
        return {r['QuestionIdentifier']: r['FreeText']}


class TqdmUpTo(tqdm):
    """Provides `update_to(n)` which uses `tqdm.update(delta_n)`."""

    def update_to(self, n=1, total=None):
        """
        b  : int, optional
            Number of blocks transferred so far [default: 1].
        bsize  : int, optional
            Size of each block (in tqdm units) [default: 1].
        tsize  : int, optional
            Total size (in tqdm units). If [default: None] remains unchanged.
        """
        if total is not None:
            self.total = total
        self.update(n - self.n)  # will also set self.n = b * bsize


AnswerReport = collections.namedtuple('AnswerReport', 'deemed_acceptable requester_feedback')


def check_task_attributes_validity(task_attributes):
    changed_attributes = set(task_attributes.keys())

    if changed_attributes != mandatory_hit_attributes:
        raise RuntimeError(f'The following mandatory hit attributes have not been defined: {mandatory_hit_attributes - changed_attributes}')

    # mandatory_attributes_change_mask = [key in task_attributes for key in mandatory_attributes]
    # if any(mandatory_attributes_change_mask) and not all(mandatory_attributes_change_mask):
    #     raise RuntimeError('If any mandatory_attributes have been changed, they all must be')


#
# class HitResults:
#     def __init__(self, assignments_launched, cooldown=1):
#         self.assignments_launched = assignments_launched
#         self.assignments_completed = 0
#         self.cooldown = cooldown
#         self.last_checked = None
#         self.parsed_result_data = []
#
#     def ready(self):
#         return self.assignments_launched == self.assignments_completed
#
#     def on_cooldown(self):
#         if self.last_checked is None:
#             return False
#         return (datetime.datetime.now() - self.last_checked).seconds < self.cooldown
#
#     def update(self, assignments, parsing_func):
#
#         self.last_checked = datetime.datetime.now()
#
#         if self.ready():
#             return
#
#         self.assignments_completed = len(assignments)
#
#         for assignment in assignments:
#
#             # Retrieve the attributes for each Assignment
#             assignment_id = assignment['AssignmentId']
#
#             # Retrieve the value submitted by the Worker from the XML
#             answer_dict = xmltodict.parse(assignment['Answer'])
#             parsed_answer = parsing_func(answer_dict)
#             parsed_answer['worker_id'] = assignment['WorkerId']
#             parsed_answer['submission_time'] = assignment['SubmitTime']
#             self.parsed_result_data.append(parsed_answer)
#
#             # Approve the Assignment (if it hasn't been already)
#             if assignment['AssignmentStatus'] == 'Submitted':
#                 answer_report = self.acceptable_answer(parsed_answer)
#                 if answer_report.deemed_acceptable:
#                     self.boto_client.approve_assignment(
#                         AssignmentId=assignment_id,
#                         OverrideRejection=False
#                     )
#                 else:
#                     self.boto_client.reject_assignment(
#                         AssignmentId=assignment_id,
#                         RequesterFeedback=answer_report.requester_feedback


class Hit(ABC):
    @abstractmethod
    def name(self):
        pass

    @abstractmethod
    def template_filename(self):
        pass

    exclude_additional_properties_from_indexes = False

    def __init__(self, hit_attributes, templates_folder='hit templates', production=False):  # @todo: nested pbars per hit

        self.exclude_additional_properties_from_indexes = None
        if production:
            check_task_attributes_validity(hit_attributes)

        hit_attributes = {  # task attributes are inherent to hits with the same name (classes)
            'Title': 'Complete a task',
            'Keywords': 'everything, nothing',
            'Description': 'Complete a given task',
            'AssignmentDurationInSeconds': 60,
            'LifetimeInSeconds': HOUR * 24,
            'Reward': 0.01,
            **hit_attributes,
        }

        # make sure reward is properly formatted
        hit_attributes['Reward'] = str(round(float(hit_attributes['Reward']), 2))
        self.hit_attributes = hit_attributes

        self.template_location = os.path.join(templates_folder, self.template_filename)
        self.production = production

        self.mturk_environment = MTURK_ENVIRONMENTS["production"] if self.production else MTURK_ENVIRONMENTS["sandbox"]

        self.boto_session = boto3.Session(profile_name=MTURK_PROFILE_NAME)
        self.boto_client = self.boto_session.client(
            service_name=MTURK_PROFILE_NAME,
            region_name=MTURK_REGION_NAME,
            endpoint_url=self.mturk_environment['endpoint'],
        )

        self.launched_instances = dict()
        self.hit_ids = []

        self.assignments_launched = None
        self.preview_links = []

        self.current_hit_params = None

    def launch_batch(self, hit_paramses: Sequence[Dict], max_assignments=1, datastore_client=None):
        self.hit_attributes['MaxAssignments'] = max_assignments

        self.assignments_launched = 0

        batch_id = str(uuid.uuid4())

        for hit_params in hit_paramses:  # the extra es suffix denotes a collection of collections

            self.current_hit_params = hit_params

            request_token = str(uuid.uuid4())  # uniquely identifies the parameters used to create this hit

            requester_annotation = json.dumps(dict(
                request_token=request_token,
                batch_id=batch_id,
            ))

            try:  # todo try loop if problem with same UniqueRequestToken=param_id
                response = self.boto_client.create_hit(
                    **self.hit_attributes,
                    Question=self.get_hit_xml(hit_params),
                    RequesterAnnotation=requester_annotation,
                    UniqueRequestToken=request_token
                )

                assert response['ResponseMetadata']['HTTPStatusCode'] == 200
                # response['ResponseMetadata']['RetryAttempts']
            except Exception as e:
                raise e

            self.assignments_launched += self.hit_attributes['MaxAssignments']

            del response['HIT']['Question']

            hit_id = response['HIT']['HITId']
            self.hit_ids.append(hit_id)

            hit_type_id = response['HIT']['HITTypeId']

            preview_link = self.mturk_environment['preview'] + "?groupId={}".format(hit_type_id)
            self.preview_links.append(preview_link)

            properties_to_be_excluded_from_indexes = ['creation_response', 'results']

            if not self.exclude_additional_properties_from_indexes:
                properties_to_be_excluded_from_indexes.append('additional_properties')

            self.launched_instances[hit_id] = dict(  # work with an entire local version of his + assignments_launhced etc
                hit_id=hit_id,
                hit_batch_id=batch_id,
                hit_name=self.name,
                request_token=request_token,
                creation_time=response['HIT']['CreationTime'],
                active=True,
                creation_response=response,
                assignments_launched=max_assignments,
                assignments_completed=0,
                assignment_ids_parsed=[],
                results_ready=False,
                results=[],
                status=None,
                preview_link=preview_link,
                production=self.production,
                hit_params=hit_params,
                additional_properties=self.get_additional_entity_properties()
            )

            if datastore_client is not None:
                database_entry = datastore.Entity(
                    datastore_client.key('hit', hit_id),
                    exclude_from_indexes=('creation_response', 'results')
                )

                del response['HIT']['Question']

                database_entry.update(self.launched_instances[hit_id])
                datastore_client.put(database_entry)

        if not self.production:
            print("You can view the HITs here:")
            print(self.preview_links)
            print(self.hit_ids)

        self.update_results()

    def get_hit_xml(self, params):
        html_layout = open(self.template_location, 'r').read()
        hit_question_xml = QUESTION_XML.format(html_layout)
        for k, v in params.items():  # todo: handle with jninja (see WordEmphasisHitBatch)
            hit_question_xml = hit_question_xml.replace(f'${{{k}}}', v)

        filtered_hit_question_xml = filter_non_ascii(hit_question_xml)
        if filtered_hit_question_xml != hit_question_xml:
            raise RuntimeWarning('some non ascii characters were removed from question xml')

        return filtered_hit_question_xml

    def parse_answers(self, answer_dict):
        return answer_dict

    def get_assignments_by_hit_id(self, hit_id):

        hit = self.boto_client.get_hit(HITId=hit_id)

        self.launched_instances[hit_id]['status'] = hit['HIT']['HITStatus']  # Get a list of the Assignments that have been submitted

        assignments = []

        list_assignments_response = self.boto_client.list_assignments_for_hit(
            HITId=hit_id,
            AssignmentStatuses=['Submitted', 'Approved', 'Rejected'],
        )

        assignments += list_assignments_response['Assignments']

        while 'NextToken' in list_assignments_response:
            list_assignments_response = self.boto_client.list_assignments_for_hit(
                HITId=hit_id,
                AssignmentStatuses=['Submitted', 'Approved', 'Rejected'],
                NextToken=list_assignments_response['NextToken']
            )

            assignments += list_assignments_response['Assignments']

        return assignments

    def update_results(self, datastore_client=None):

        for hit_id in self.launched_instances.keys():
            assignments = self.get_assignments_by_hit_id(hit_id)

            assignments_completed = len(assignments)
            self.launched_instances[hit_id]['assignments_completed'] = assignments_completed
            self.launched_instances[hit_id]['ready'] = assignments_completed == self.launched_instances[hit_id]['assignments_launched']

            parsed_results = []
            for assignment in assignments:

                # Retrieve the attributes for each Assignment
                assignment_id = assignment['AssignmentId']

                if assignment_id in self.launched_instances[hit_id]['assignment_ids_parsed']:
                    continue

                # Retrieve the value submitted by the Worker from the XML
                answer_dict = xmltodict.parse(assignment['Answer'])
                parsed_result = self.parse_answers(answer_dict)
                parsed_result['worker_id'] = assignment['WorkerId']
                parsed_result['submission_time'] = assignment['SubmitTime']
                parsed_results.append(parsed_result)

                # Approve the Assignment (if it hasn't been already)
                if assignment['AssignmentStatus'] == 'Submitted':
                    answer_report = self.acceptable_answer(parsed_result)
                    if answer_report.deemed_acceptable:
                        self.boto_client.approve_assignment(
                            AssignmentId=assignment_id,
                            OverrideRejection=False
                        )
                    else:
                        self.boto_client.reject_assignment(
                            AssignmentId=assignment_id,
                            RequesterFeedback=answer_report.requester_feedback
                        )

                    self.launched_instances[hit_id]['assignment_ids_parsed'].append(assignment_id)

            self.launched_instances[hit_id]['results'] += parsed_results

            if datastore_client is not None:
                query = datastore_client.query(kind='hit')
                query.add_filter('hit_id', '=', hit_id)
                hit_entity = list(query.fetch())[0]
                hit_entity.update(self.launched_instances[hit_id])
                datastore_client.put(hit_entity)

    def acceptable_answer(self, parsed_answer):
        return AnswerReport(True, '')

    def prepare_results(self, results):
        return results

    def completed(self, update=True):

        if update:
            self.update_results()

        c = 0
        for item in self.results:
            c += self.boto_client.list_assignments_for_hit(
                HITId=item['hit_id'],
                AssignmentStatuses=['Submitted', 'Approved', 'Rejected'],
            )['NumResults']

        return c == self.assignments_launched and self.assignments_launched > 0

    def poll(self):

        c = 0

        for item in self.results:
            c += self.boto_client.list_assignments_for_hit(
                HITId=item['hit_id'],
                AssignmentStatuses=['Submitted', 'Approved', 'Rejected'],
            )['NumResults']

        return c

    def pbar(self):

        pbar_listener(self.hit_ids, self.assignments_launched, self.production)
        self.update_results()

        # proc = mp.Process(target=pbar_listener, args=(self.hit_ids, self.num_launched, self.production))
        # proc.start()
        # proc.join()

    def get_additional_entity_properties(self):
        return dict()


def pbar_listener(hit_ids, total, production):
    phb = PreexistingHit(hit_ids=hit_ids, production=production)

    phb.update_results()

    c = 0

    with TqdmUpTo(total=total) as pbar:

        while c != total:
            c = 0

            for item in phb.results:
                c += phb.boto_client.list_assignments_for_hit(
                    HITId=item['hit_id'],
                    AssignmentStatuses=['Submitted', 'Approved', 'Rejected'],
                )['NumResults']

            pbar.update_to(c)
            if c != total:
                sleep(3)


class PreexistingHit(Hit):

    def __init__(self, hit_ids, production=False, update=True):
        super().__init__(
            template_location=None,
            hit_attributes=None,
            production=production
        )
        self.hit_ids = hit_ids

        self.results = [{'hit_id': hit_id} for hit_id in hit_ids]
        self.num_launched = len(hit_ids)

        if update:
            self.update_results()


def extract_free_text(answer_dict):
    if type(answer_dict['QuestionFormAnswers']['Answer']) is list:
        return {item['QuestionIdentifier']: item['FreeText'] for item in answer_dict['QuestionFormAnswers']['Answer']}
    else:
        r = answer_dict['QuestionFormAnswers']['Answer']
        return {r['QuestionIdentifier']: r['FreeText']}


def hit_is_completed(hit_id, production=False):
    return PreexistingHit(hit_ids=[hit_id], production=production, update=False).completed()


class HitObserver:
    def __init__(self, datastore_client, production=False):
        self.datastore_client = datastore_client
        self.production = production

    def update(self):
        query = self.datastore_client.query(kind='hit')
        query.add_filter('active', '=', True)

        hit_entities = query.fetch()

        batch = self.datastore_client.batch()

        with batch:
            for hit_entity in hit_entities:
                hb = PreexistingHit(hit_ids=[hit_entity['hit_id']], production=self.production)
                if hb.completed():
                    hit_entity.update(dict(
                        active=False,
                        results=hb.results
                    ))
                    batch.put(hit_entity)


# todo: use decorator


def delete_all_hits(production, loop=True, attempts=100):
    boto_session = boto3.Session(profile_name=MTURK_PROFILE_NAME)
    mturk = boto_session.client(
        service_name=MTURK_PROFILE_NAME,
        region_name=MTURK_REGION_NAME,
        endpoint_url=MTURK_ENVIRONMENTS["production"]['endpoint'] if production else MTURK_ENVIRONMENTS["sandbox"]['endpoint'],
    )

    if not loop:
        attempts = 1

    while attempts != 0:
        attempts -= 1
        hits = mturk.list_hits()['HITs']
        num_hits = len(hits)
        if num_hits == 0:
            print('no hits present')
            return

        print(f'found {num_hits} hits')

        for hit in hits:
            hit_id = hit['HITId']
            print('HITId:', hit_id)

            # Get HIT status
            status = mturk.get_hit(HITId=hit_id)['HIT']['HITStatus']
            print('HITStatus:', status)

            # If HIT is active then set it to expire immediately
            if status == 'Assignable':
                response = mturk.update_expiration_for_hit(
                    HITId=hit_id,
                    ExpireAt=datetime.datetime(2015, 1, 1)
                )

                if status == 'Submitted':
                    mturk.approve_assignment(
                        AssignmentId=hit_id,
                        OverrideRejection=False
                    )
                # Delete the HIT
            try:
                mturk.delete_hit(HITId=hit_id)
            except:
                print('Not deleted')
            else:
                print('Deleted')


def reaprove_hit(hit_id):
    boto_session = boto3.Session(profile_name=MTURK_PROFILE_NAME)

    mturk = boto_session.client(
        service_name=MTURK_PROFILE_NAME,
        region_name=MTURK_REGION_NAME,
        endpoint_url=MTURK_ENVIRONMENTS["production"]['endpoint']
    )

    mturk.approve_assignment(
        AssignmentId=hit_id,
        OverrideRejection=True
    )


class AnswerQuestionHit(Hit):
    name = 'answer question'
    template_filename = 'answer question.html'

    def __init__(self, production=False):

        task_attributes = {
            'Title': 'Answer a question',
            'Keywords': 'general knowledge, question answering',
            'Description': 'Answer a question as best as you can',  # duration etc..
        }

        if production:
            task_attributes['QualificationRequirements'] = DEFAULT_TASK_QUALIFICATIONS + [us_high_school_qualification]
        else:
            task_attributes['QualificationRequirements'] = []

        super().__init__(
            hit_attributes=task_attributes,
            production=production,
        )

    def parse_answers(self, answer_dict):
        return extract_free_text(answer_dict)


def launch_hits(hb, batch_params, production=False, title_task_attributes=None, datastore_client=None, bar=False):
    hb = hb(
        production=production,
    )

    hb.launch_batch(hit_paramses=batch_params, datastore_client=datastore_client)
    if not production:
        hb.pbar()
        print(hb.results)

    return hb.results


def launch_question_hit_batch(questions, production=False, datastore_client=None, verbose=True):
    batch_params = [{'question': q} for q in questions]
    if verbose:
        print(f'\tlaunching ShortenQuestionHitBatch')
    launch_hits(AnswerQuestionHit, batch_params, production=production, datastore_client=datastore_client)


if __name__ == "__main__":
    launch_question_hit_batch(
        questions=['what is 1 + 1?', 'what is 2 + 2?']
    )

    # while True:
    #     delete_all_hits(production=False)
    #     time.sleep(5)

    pass
