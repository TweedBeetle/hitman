import collections
import datetime
import os
import time
import uuid

import boto3
import xmltodict
from time import sleep

from tqdm import tqdm

from google.cloud import datastore

MTURK_REGION_NAME = 'us-east-1'
MINUTE = 60
HOUR = 60 * MINUTE

QUESTION_XML = """<HTMLQuestion xmlns="http://mechanicalturk.amazonaws.com/AWSMechanicalTurkDataSchemas/2011-11-11/HTMLQuestion.xsd">
        <HTMLContent><![CDATA[{}]]></HTMLContent>
        <FrameHeight>650</FrameHeight>
        </HTMLQuestion>"""

MTURK_ENVIRONMENTS = {
    "production": {
        "endpoint": "https://mturk-requester.us-east-1.amazonaws.com",
        "preview": "https://www.mturk.com/mturk/preview"
    },
    "sandbox": {
        "endpoint":
            "https://mturk-requester-sandbox.us-east-1.amazonaws.com",
        "preview": "https://workersandbox.mturk.com/mturk/preview"
    },
}
MTURK_PROFILE_NAME = 'mturk'

us_high_school_qualification = {
    'QualificationTypeId': "3D95X78DG55X7Z43AZIOMTK5O7D8TE",  # us high school degree
    'Comparator': "EqualTo",
    'IntegerValues': [1],
    'ActionsGuarded': "DiscoverPreviewAndAccept",
}

from_america_qualification = {
    'QualificationTypeId': "00000000000000000071",  # from the us
    'Comparator': "EqualTo",
    'LocaleValues': [{
        'Country': "US",
    }],
    'ActionsGuarded': "DiscoverPreviewAndAccept",
}

STANDARD_TASK_ATTRIBUTES = [
    {
        'QualificationTypeId': "00000000000000000060",  # ok with adult  content
        'Comparator': "EqualTo",
        'IntegerValues': [1],
        'ActionsGuarded': "DiscoverPreviewAndAccept",
    },
    {
        'QualificationTypeId': "000000000000000000L0",  # percentage approved
        'Comparator': "GreaterThan",
        'IntegerValues': [97],
        'ActionsGuarded': "DiscoverPreviewAndAccept",
    },
    {
        'QualificationTypeId': "00000000000000000040",  # Worker_​NumberHITsApproved
        'Comparator': "GreaterThan",
        'IntegerValues': [100],
        'ActionsGuarded': "DiscoverPreviewAndAccept",
    },
    from_america_qualification,
    # us_high_school_qualification,
    # {
    #     'QualificationTypeId': "32HPRZ3TSLYH2064JOFRCYTWJ82KT6",  # no reddit account
    #     'Comparator': "EqualTo",
    #     'IntegerValues': [0],
    #     'ActionsGuarded': "DiscoverPreviewAndAccept",
    # },
]


def is_ascii_char(c):
    return ord(c) < 128


def filter_non_ascii(s):
    return ''.join(filter(is_ascii_char, list(s)))


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


class HitBatch:
    hit_type = 'generic'

    def __init__(self, template_location, task_attributes, production=False, hit_type=None):  # @todo: nested pbars per hit

        if task_attributes is not None:
            task_attributes['Reward'] = str(round(float(task_attributes['Reward']), 2))

        self.template_location = template_location
        self.task_attributes = task_attributes
        self.production = production
        if hit_type is None:
            if self.template_location is None:
                self.hit_type = 'unknown'
            else:
                self.hit_type = ' '.join(os.path.split(self.template_location)[-1].split()[:-1])
        else:
            self.hit_type = hit_type

        self.mturk_environment = MTURK_ENVIRONMENTS["production"] if self.production else MTURK_ENVIRONMENTS["sandbox"]

        self.boto_session = boto3.Session(profile_name=MTURK_PROFILE_NAME)
        self.boto_client = self.boto_session.client(
            service_name=MTURK_PROFILE_NAME,
            region_name=MTURK_REGION_NAME,
            endpoint_url=self.mturk_environment['endpoint'],
        )

        self.raw_results = []
        self.results = []
        self.hit_ids = []

        self.num_launched = None
        self.preview_links = []

        self.current_hit_params = None

    def launch(self, batch_params, verbose=False, datastore_client=None):

        self.num_launched = 0
        hit_batch_id = str(uuid.uuid4())
        for hit_params in batch_params:
            self.current_hit_params = hit_params

            try:
                response = self.boto_client.create_hit(
                    **self.task_attributes,
                    Question=self.get_hit_xml(hit_params)
                )
            except Exception as e:
                raise e

            self.num_launched += self.task_attributes['MaxAssignments']

            hit_id = response['HIT']['HITId']
            self.hit_ids.append(hit_id)

            hit_type_id = response['HIT']['HITTypeId']
            self.results.append({
                **hit_params,
                'hit_id': hit_id
            })

            self.preview_links.append(self.mturk_environment['preview'] + "?groupId={}".format(hit_type_id))

            if datastore_client is not None:
                database_entry = datastore.Entity(
                    datastore_client.key('hit', hit_id),
                    exclude_from_indexes=('creation_response', 'results')
                )

                del response['HIT']['Question']

                properties = dict(
                    hit_batch_id=hit_batch_id,
                    hit_id=hit_id,
                    hit_type=self.hit_type,
                    creation_time=datetime.datetime.now(),
                    active=True,
                    creation_response=response,
                    results=None,
                    preview_link=self.preview_links[-1],
                    production=self.production,
                    **self.get_additional_entity_properties(),
                )

                database_entry.update(properties)
                datastore_client.put(database_entry)

        if not self.production:
            print("You can view the HITs here:")
            print(self.preview_links)
            print(self.hit_ids)

        self.update_results()

    def get_hit_xml(self, params):
        html_layout = open(self.template_location, 'r').read()
        hit_question_xml = QUESTION_XML.format(html_layout)
        for k, v in params.items():
            hit_question_xml = hit_question_xml.replace(f'${{{k}}}', v)

        filtered_hit_question_xml = filter_non_ascii(hit_question_xml)
        if filtered_hit_question_xml != hit_question_xml:
            pass  # todo: log

        return filtered_hit_question_xml

    def parse_answers(self, answer_dict):
        return answer_dict

    def update_results(self, datastore_client=None):

        for result_item in self.results:
            # Get the status of the HIT
            hit = self.boto_client.get_hit(HITId=result_item['hit_id'])
            result_item['status'] = hit['HIT']['HITStatus']  # Get a list of the Assignments that have been submitted
            assignments_list = self.boto_client.list_assignments_for_hit(
                HITId=result_item['hit_id'],
                AssignmentStatuses=['Submitted', 'Approved', 'Rejected'],
            )
            assignments = assignments_list['Assignments']
            result_item['assignments_submitted_count'] = len(assignments)

            parsed_answers = []
            for assignment in assignments:

                # Retrieve the attributes for each Assignment
                assignment_id = assignment['AssignmentId']

                # Retrieve the value submitted by the Worker from the XML
                answer_dict = xmltodict.parse(assignment['Answer'])
                parsed_answer = self.parse_answers(answer_dict)
                parsed_answer['worker_id'] = assignment['WorkerId']
                parsed_answer['submission_time'] = assignment['SubmitTime']
                parsed_answers.append(parsed_answer)

                # Approve the Assignment (if it hasn't been already)
                if assignment['AssignmentStatus'] == 'Submitted':
                    answer_report = self.acceptable_answer(parsed_answer)
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

            # Add the answers that have been retrieved for this result_item
            result_item['answers'] = parsed_answers

            if datastore_client is not None:
                query = datastore_client.query(kind='hit')
                query.add_filter('hit_id', '=', result_item['hit_id'])
                hit_entity = list(query.fetch())[0]
                hit_entity.update(dict(results=self.prepare_results(result_item)))
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

        return c == self.num_launched and self.num_launched > 0

    def poll(self):

        c = 0

        for item in self.results:
            c += self.boto_client.list_assignments_for_hit(
                HITId=item['hit_id'],
                AssignmentStatuses=['Submitted', 'Approved', 'Rejected'],
            )['NumResults']

        return c

    def pbar(self):

        pbar_listener(self.hit_ids, self.num_launched, self.production)
        self.update_results()

        # proc = mp.Process(target=pbar_listener, args=(self.hit_ids, self.num_launched, self.production))
        # proc.start()
        # proc.join()

    def get_additional_entity_properties(self):
        return dict()


def pbar_listener(hit_ids, total, production):
    phb = PreexistingHitBatch(hit_ids=hit_ids, production=production)

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


class PreexistingHitBatch(HitBatch):

    def __init__(self, hit_ids, production=False, update=True):
        super().__init__(
            template_location=None,
            task_attributes=None,
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
    return PreexistingHitBatch(hit_ids=[hit_id], production=production, update=False).completed()


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
                hb = PreexistingHitBatch(hit_ids=[hit_entity['hit_id']], production=self.production)
                if hb.completed():
                    hit_entity.update(dict(
                        active=False,
                        results=hb.results
                    ))
                    batch.put(hit_entity)


# todo: use decorator


def delete_all_hits(production):
    boto_session = boto3.Session(profile_name=MTURK_PROFILE_NAME)
    mturk = boto_session.client(
        service_name=MTURK_PROFILE_NAME,
        region_name=MTURK_REGION_NAME,
        endpoint_url=MTURK_ENVIRONMENTS["production"]['endpoint'] if production else MTURK_ENVIRONMENTS["sandbox"]['endpoint'],
    )

    for item in mturk.list_hits()['HITs']:
        hit_id = item['HITId']
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


if __name__ == "__main__":
    # launch_emphasis_hit_batch(1)

    # datastore_client = get_datastore_client()
    # launch_shorten_question_hit_batch(submission_id='2np694', production=False)

    # launch_video_tags_hit_batch(submission_id='2np694', production=False)
    # launch_word_emphasis_hit_batch(submission_id='2np694', title='Test ententanc ethis is', production=False)

    # observer = HitObserver(datastore_client, production=False)
    # observer.update()

    # reaprove_hit('335VBRURDIZ4PRG9JKX702T0Z7PE9T')

    # while True:
    #     delete_all_hits(True)
    #     time.sleep(5)

    # launch_flavour_image_hit_batch('2np694', production=False, bar=True)
    # launch_image_background_hit_batch('asdas', image_url='https://cdn.shopify.com/s/files/1/1234/1684/products/print_all_over_me_3_t-shirt_0000000p-one-true-god.jpg',
    #                                   production=False, bar=True)
    # delete_all_hits(True)

    # launch_title_rating_hit_batch([])

    urls = [
        'https://cdn.shopify.com/s/files/1/1234/1684/products/print_all_over_me_3_t-shirt_0000000p-one-true-god.jpg',
        'https://cdn.shopify.com/s/files/1/1234/1684/products/print_all_over_me_3_t-shirt_0000000p-one-true-god.jpg',
        'https://cdn.shopify.com/s/files/1/1234/1684/products/print_all_over_me_3_t-shirt_0000000p-one-true-god.jpg',
    ]

    # launch_thumbnail_rating_hit_batch('asdas', image_urls=urls,
    #                                   production=False, bar=True)

    pass
