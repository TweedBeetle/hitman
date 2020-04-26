import collections
import json
import os

from jinja2 import Environment, FileSystemLoader, select_autoescape
from config import DEFAULT_HIT_LIFETIME, DEFAULT_APPROVAL_DELAY, _DEFAULT_REWARD_PER_SECOND, MINUTE, DEFAULT_TASK_QUALIFICATIONS, us_high_school_qualification


# from nlp import split_sentence
# from reddit import get_submission_by_id
from utils import filter_non_ascii



class AlienAnswersHitBatch(HitBatch):  # todo: introduce companion datastore entity to store metadata (args)
    def __init__(self, template_location, task_attributes, submission_id, production=False, hit_type=None):
        task_attributes.update({
            'LifetimeInSeconds': DEFAULT_HIT_LIFETIME,
            'AutoApprovalDelayInSeconds': DEFAULT_APPROVAL_DELAY,
        })

        if 'Reward' not in task_attributes.keys():
            task_attributes.update({
                'Reward': task_attributes['AssignmentDurationInSeconds'] * _DEFAULT_REWARD_PER_SECOND,
            })

        super().__init__(template_location=template_location, task_attributes=task_attributes, production=production, hit_type=hit_type)
        self.submission_id = submission_id

    def get_additional_entity_properties(self):
        return dict(submission_id=self.submission_id)


def create_aa_hit_batch_from_hit_ids(hb, hit_ids, submission_id, production, update=True):
    hb = hb(
        task_attributes={},
        submission_id=submission_id,
        production=production
    )

    hb.hit_ids = hit_ids

    hb.results = [{'hit_id': hit_id} for hit_id in hit_ids]
    hb.assignments_launched = len(hit_ids)

    if update:
        hb.update_results()

    return hb


class VideoTitleHitBatch(AlienAnswersHitBatch):
    hit_type = 'video title'
    assignment_duration = MINUTE * 3

    def __init__(self, task_attributes, submission_id, production=False):
        task_attributes = {
            **task_attributes,
            'Title': 'Create a title to a youtube video',
            'Keywords': 'rephrasing, sentence shortening, writing, language, titling',
            'Description': 'Given a question, come up with a title to a youtube video who\'s content is comprised of answers to the given question',
            'AssignmentDurationInSeconds': VideoTitleHitBatch.assignment_duration,
            'Reward': '0.04',
        }

        if production:
            task_attributes['QualificationRequirements'] = DEFAULT_TASK_QUALIFICATIONS + [us_high_school_qualification]

        super().__init__(
            template_location=os.path.join(get_relative_project_root(), 'hit templates', 'video title hit.html'),
            task_attributes=task_attributes,
            submission_id=submission_id,
            production=production,
        )

    def parse_answers(self, answer_dict):
        return extract_free_text(answer_dict)


class VideoTagsHitBatch(AlienAnswersHitBatch):
    hit_type = 'video tags'
    assignment_duration = MINUTE * 2

    def __init__(self, task_attributes, submission_id, production=False):
        task_attributes = {
            **task_attributes,
            'Title': 'Create a tags for a youtube video',
            'Keywords': 'tagging, sentence tagging',
            'Description': 'Given a question, come up with tags to a youtube video who\'s content is comprised of answers to the given question',
            'AssignmentDurationInSeconds': VideoTagsHitBatch.assignment_duration,
            'Reward': '0.02',
        }

        if production:
            task_attributes['QualificationRequirements'] = DEFAULT_TASK_QUALIFICATIONS + [us_high_school_qualification]

        super().__init__(
            template_location=os.path.join(get_relative_project_root(), 'hit templates', 'video tags hit.html'),
            task_attributes=task_attributes,
            submission_id=submission_id,
            production=production,
        )

    def parse_answers(self, answer_dict):
        video_tags = list(filter(lambda s: s, [s.strip() for s in extract_free_text(answer_dict)['video tags'].split(',')]))
        return {'video tags': video_tags}


class WordEmphasisHitBatch(AlienAnswersHitBatch):
    hit_type = 'word emphasis'
    assignment_duration = MINUTE * 2

    def __init__(self, task_attributes, submission_id, production=False):
        complete_task_attributes = {
            **task_attributes,
            'Title': 'Decide which words in a title carry significant meaning',
            'Keywords': 'language, emphasis',
            'Description': 'Decide which words in a title should be emphasized in order to clarify meaning',
            'AssignmentDurationInSeconds': WordEmphasisHitBatch.assignment_duration,
            'Reward': '0.03',
        }

        if production:
            complete_task_attributes['QualificationRequirements'] = DEFAULT_TASK_QUALIFICATIONS

        super().__init__(
            template_location=os.path.join(get_relative_project_root(), 'hit templates', 'word emphasis hit.html'),
            task_attributes=complete_task_attributes,
            submission_id=submission_id,
            production=production,
        )

    def get_hit_xml(self, params):

        template_env = Environment(
            loader=FileSystemLoader(os.path.join(get_relative_project_root(), 'hit templates')),
            autoescape=select_autoescape(['html'])
        )

        hit_template = template_env.get_template('word emphasis hit.html')

        # words = get_tokens_text(params['title'])
        words = params['title'].split()

        html = hit_template.render(words=words, indices=range(len(words)))

        hit_question_xml = QUESTION_XML.format(html)

        filtered_hit_question_xml = filter_non_ascii(hit_question_xml)
        if filtered_hit_question_xml != hit_question_xml:
            pass  # todo: log

        return filtered_hit_question_xml

    def parse_answers(self, answer_dict):
        if type(answer_dict) != collections.OrderedDict:
            indices = [
                int(answer_dict['QuestionFormAnswers']['Answer']['QuestionIdentifier'].split()[0])
            ]
        else:
            indices = [
                int(item['QuestionIdentifier'].split()[0]) for item in
                filter(lambda x: x['QuestionIdentifier'] != 'feedback', answer_dict['QuestionFormAnswers']['Answer'])
            ]

        num_indices = max(indices) + 1

        d = {
            'feedback': '',
            'emphasis_mask': [0] * num_indices
        }

        if type(answer_dict) != collections.OrderedDict:
            space_split = answer_dict['QuestionFormAnswers']['Answer']['QuestionIdentifier'].split()
            ind = int(space_split[0])
            d['emphasis_mask'][ind] = answer_dict['QuestionFormAnswers']['Answer']['FreeText'] == 'true'
        else:
            for item in answer_dict['QuestionFormAnswers']['Answer']:
                if item['QuestionIdentifier'] == 'feedback':
                    d['feedback'] = item['FreeText']
                else:
                    space_split = item['QuestionIdentifier'].split()
                    ind = int(space_split[0])
                    d['emphasis_mask'][ind] = item['FreeText'] == 'true'

        return d

    def get_additional_entity_properties(self):
        return dict(
            submission_id=self.submission_id,
            title=self.current_hit_params['title'],
        )


class FlavourImageHitBatch(AlienAnswersHitBatch):
    hit_type = 'flavour image'
    assignment_duration = MINUTE * 10

    def __init__(self, task_attributes, submission_id, production=False):
        task_attributes = {
            **task_attributes,
            'Title': 'Find an image for a youtube thumbnail',
            'Keywords': 'image search, thumbnail, youtube',
            'Description': 'Find the url of an image to be used for a youtube thumbnail, based on the video title',
            'AssignmentDurationInSeconds': FlavourImageHitBatch.assignment_duration,
            'Reward': '0.12',
        }

        if production:
            task_attributes['QualificationRequirements'] = DEFAULT_TASK_QUALIFICATIONS

        super().__init__(
            template_location=os.path.join(get_relative_project_root(), 'hit templates', 'flavour image hit.html'),
            task_attributes=task_attributes,
            submission_id=submission_id,
            production=production,
        )

    def parse_answers(self, answer_dict):
        return extract_free_text(answer_dict)

    def prepare_results(self, results):
        try:
            results['answers'][0]['image location'] = results['answers'][0]['image location'][:250]
        except IndexError:
            pass
        return results

    def acceptable_answer(self, parsed_answer):
        image_location = parsed_answer['image location']
        if is_image_and_ready(image_location) or image_location.lower().endswith('.jpg') or image_location.lower().endswith('.png'):
            if not is_image_and_ready(image_location):
                print(f'would have liked to reject {image_location}')
            return AnswerReport(True, '')
        else:
            print(f'rejecting {image_location}')
            return AnswerReport(False, f'The image location you submitted does not link to a '
                                       f'valid image as described in the assignment description: \n{image_location}')
            # todo: log


class ImageBackgroundHitBatch(AlienAnswersHitBatch):
    hit_type = 'image background'
    assignment_duration = MINUTE * 5

    def __init__(self, task_attributes, submission_id, production=False):
        task_attributes = {
            **task_attributes,
            'Title': 'Provide information on the background of an image',
            'Keywords': 'image description, background tagging',
            'Description': 'Identify the background of an image and check if it is of a single solid color',
            'AssignmentDurationInSeconds': ImageBackgroundHitBatch.assignment_duration,
            'Reward': '0.05',
        }

        if production:
            task_attributes['QualificationRequirements'] = DEFAULT_TASK_QUALIFICATIONS

        super().__init__(
            template_location=os.path.join(get_relative_project_root(), 'hit templates', 'image background hit.html'),
            task_attributes=task_attributes,
            submission_id=submission_id,
            production=production,
        )

    def parse_answers(self, answer_dict):
        extracted_free_text = extract_free_text(answer_dict)
        keypoint_string = extracted_free_text['annotatedResult.keypoints']
        keypoint_dicts = json.loads(keypoint_string)
        has_background = len(keypoint_dicts) != 0
        if has_background:
            background_x = keypoint_dicts[0]['x']
            background_y = keypoint_dicts[0]['y']
        else:
            background_x = None
            background_y = None

        background_solid = extracted_free_text['background_is_solid'].lower().strip() == 'yes'

        del extracted_free_text['annotatedResult.keypoints']
        del extracted_free_text['background_is_solid']

        extracted_free_text['has_background'] = has_background
        extracted_free_text['background_coords'] = [background_x, background_y]
        extracted_free_text['background_solid'] = background_solid

        return extracted_free_text

    def get_additional_entity_properties(self):
        return dict(
            submission_id=self.submission_id,
            image_location=self.current_hit_params['image_url'],
        )


class ThumbnailRatingHitBatch(AlienAnswersHitBatch):
    hit_type = 'thumbnail rating'
    assignment_duration = MINUTE * 2

    def __init__(self, task_attributes, submission_id, production=False):
        task_attributes = {
            **task_attributes,
            'Title': 'Rate 3 video thumbnails',
            'Keywords': 'image rating',
            'Description': 'Rate 3 youtube video thumbnails from best to worst',
            'AssignmentDurationInSeconds': ThumbnailRatingHitBatch.assignment_duration,
            'Reward': '0.02'
        }

        if production:
            task_attributes['QualificationRequirements'] = DEFAULT_TASK_QUALIFICATIONS

        super().__init__(
            template_location=os.path.join(get_relative_project_root(), 'hit templates', 'thumbnail rating hit.html'),
            task_attributes=task_attributes,
            submission_id=submission_id,
            production=production,
        )

    def parse_answers(self, answer_dict):
        return extract_free_text(answer_dict)

    def get_additional_entity_properties(self):
        return dict(
            submission_id=self.submission_id,
            image_urls=list(self.current_hit_params.values()),
        )


class TitleRatingHitBatch(AlienAnswersHitBatch):
    hit_type = 'title rating'
    assignment_duration = MINUTE * 2

    def __init__(self, task_attributes, submission_id, production=False):
        task_attributes = {
            **task_attributes,
            'Title': 'Rate 3 video titles',
            'Keywords': 'titling, rating',
            'Description': 'Rate 3 youtube video titles from best to worst',
            'AssignmentDurationInSeconds': TitleRatingHitBatch.assignment_duration,
            'Reward': '0.02',
        }

        if production:
            task_attributes['QualificationRequirements'] = DEFAULT_TASK_QUALIFICATIONS

        super().__init__(
            template_location=os.path.join(get_relative_project_root(), 'hit templates', 'title rating hit.html'),
            task_attributes=task_attributes,
            submission_id=submission_id,
            production=production,
        )

    def parse_answers(self, answer_dict):
        return extract_free_text(answer_dict)

    def get_additional_entity_properties(self):
        return dict(
            submission_id=self.submission_id,
            video_titles=[self.current_hit_params[k] for k in filter(lambda k: k != 'question', self.current_hit_params.keys())],
        )



def launch_video_title_hit_batch(submission_id, production=False, datastore_client=None, verbose=True):
    batch_params = [{'question': get_submission_by_id(submission_id).title}]
    if verbose:
        print(f'\tlaunching VideoTitleHitBatch')
    launch_aa_hit_batch(VideoTitleHitBatch, batch_params, submission_id=submission_id, production=production, datastore_client=datastore_client)


def launch_video_tags_hit_batch(submission_id, production=False, datastore_client=None, verbose=True):
    batch_params = [{'question': get_submission_by_id(submission_id).title}]
    if verbose:
        print(f'\tlaunching VideoTagsHitBatch')
    launch_aa_hit_batch(VideoTagsHitBatch, batch_params, submission_id=submission_id, production=production, datastore_client=datastore_client)


def launch_word_emphasis_hit_batch(submission_id, title, production=False, datastore_client=None, verbose=True):
    batch_params = [{'title': title}]
    if verbose:
        print(f'\tlaunching WordEmphasisHitBatch')
    launch_aa_hit_batch(WordEmphasisHitBatch, batch_params, submission_id=submission_id, production=production, datastore_client=datastore_client)


def launch_flavour_image_hit_batch(submission_id, production=False, datastore_client=None, bar=False, verbose=True):
    batch_params = [{'title': get_submission_by_id(submission_id).title}]
    if verbose:
        print(f'\tlaunching FlavourImageHitBatch')
    launch_aa_hit_batch(FlavourImageHitBatch, batch_params, submission_id=submission_id, production=production,
                        datastore_client=datastore_client, bar=bar)


def launch_image_background_hit_batch(submission_id, image_url, production=False, datastore_client=None, bar=False, verbose=True):
    batch_params = [{'image_url': image_url}]
    if verbose:
        print(f'\tlaunching ImageBackgroundHitBatch')
    launch_aa_hit_batch(ImageBackgroundHitBatch, batch_params, submission_id=submission_id, production=production,
                        datastore_client=datastore_client, bar=bar)


def launch_thumbnail_rating_hit_batch(submission_id, image_urls, production=False, datastore_client=None, bar=False, verbose=True):
    batch_params = [{'image_url_1': image_urls[0], 'image_url_2': image_urls[1], 'image_url_3': image_urls[2]}]
    if verbose:
        print(f'\tlaunching ThumbnailRatingHitBatch')
    launch_aa_hit_batch(ThumbnailRatingHitBatch, batch_params, submission_id=submission_id, production=production,
                        datastore_client=datastore_client, bar=bar)


def launch_title_rating_hit_batch(submission_id, video_titles, production=False, datastore_client=None, bar=False, verbose=True):
    batch_params = [{'question': get_submission_by_id(submission_id).title, 'title_1': video_titles[0], 'title_2': video_titles[1], 'title_3': video_titles[2]}]
    if verbose:
        print(f'\tlaunching TitleRatingHitBatch')
    launch_aa_hit_batch(TitleRatingHitBatch, batch_params, submission_id=submission_id, production=production,
                        datastore_client=datastore_client, bar=bar)


def get_emphasis_mask_from_hit(thumbnail_title):
    title_task_attributes = {
        'MaxAssignments': 1,
        'LifetimeInSeconds': HOUR * 5,
        'AutoApprovalDelayInSeconds': HOUR * 1,
    }

    batch_params = [{'title': thumbnail_title}]
    ehb = WordEmphasisHitBatch(task_attributes=title_task_attributes, production=True)
    ehb.launch(batch_params=batch_params, verbose=False)
    ehb.pbar()
    tokens = split_sentence(thumbnail_title)
    emphasis_mask = ehb.results[0]['answers'][0]['emphasis_mask']
    return emphasis_mask, tokens


if __name__ == "__main__":

    pass
    # while True:
    #     try:
    #         delete_all_hits(True)
    #     except:
    #         pass
    #     finally:
    #         time.sleep(60 * 5)
