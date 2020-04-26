import datetime
import random
import io
import time

import boto3

from competition import CompetitionFFAWTA

# from config.cfg import NUM_ALL_TIME_TOP_POSTS_PER_DAY, NUM_WEEKLY_TOP_POSTS_PER_DAY, NUM_FLAVOUR_IMAGE_HITS, NUM_THUMBNAIL_RATING_HITS, \
#     NUM_VIDEO_TITLE_HITS, NUM_VIDEO_TITLE_RATING_HITS
# from database import get_datastore_client, get_storage_client
# from hit templates import MTURK_REGION_NAME, MTURK_ENVIRONMENTS, MTURK_PROFILE_NAME
# from mturk import create_aa_hit_batch_from_hit_ids, VideoTitleHitBatch, VideoTagsHitBatch, WordEmphasisHitBatch, FlavourImageHitBatch, ImageBackgroundHitBatch, \
#     ThumbnailRatingHitBatch, TitleRatingHitBatch, launch_video_title_hit_batch, launch_video_tags_hit_batch, launch_word_emphasis_hit_batch, \
#     launch_flavour_image_hit_batch, launch_image_background_hit_batch, launch_thumbnail_rating_hit_batch, launch_title_rating_hit_batch
# from reddit import get_askreddit, get_submission_by_id
# from google.cloud import datastore
#
# from thumbnail import get_thumbnail, FlavourImage
# from utils import is_image_and_ready, image_from_url


# todo: only recheck non completed submission_ids

def select_posts_for_week():
    datastore_client = get_datastore_client()
    total_posts_for_week = (NUM_ALL_TIME_TOP_POSTS_PER_DAY + NUM_WEEKLY_TOP_POSTS_PER_DAY) * 7

    all_time_top_posts_query = datastore_client.query(kind='askreddit_top_post')
    all_time_top_posts_query.add_filter('used', '=', False)

    if NUM_ALL_TIME_TOP_POSTS_PER_DAY > 0:
        selected_all_time_top_posts = list(all_time_top_posts_query.fetch(limit=7 * NUM_ALL_TIME_TOP_POSTS_PER_DAY))
    else:
        selected_all_time_top_posts = []

    batch = datastore_client.batch()
    with batch:
        for post in selected_all_time_top_posts:
            post.update(dict(used=True))
            batch.put(post)

    num_weekly_top_posts_for_week = total_posts_for_week - len(selected_all_time_top_posts)

    if num_weekly_top_posts_for_week != NUM_WEEKLY_TOP_POSTS_PER_DAY * 7:
        pass  # todo  we've run out of all time posts -> log

    # todo: check filter for previously used top weekly submissions
    askreddit = get_askreddit()
    selected_weekly_top_submissions = list(askreddit.top('week', limit=num_weekly_top_posts_for_week))
    random.shuffle(selected_weekly_top_submissions)

    batch = datastore_client.batch()
    with batch:
        for post in selected_all_time_top_posts:
            database_entry = datastore.Entity(
                datastore_client.key('selected_post', post.key.name),
            )

            properties = dict(
                all_time_top_post=True,
                submission_id=post.key.name
            )

            database_entry.update(properties)
            batch.put(database_entry)

        for submission in selected_weekly_top_submissions:
            database_entry = datastore.Entity(
                datastore_client.key('selected_post', submission.id),
            )

            properties = dict(
                all_time_top_post=False,
                submission_id=submission.id
            )

            database_entry.update(properties)
            batch.put(database_entry)


def archived_datastore_entity(entity, datastore_client):
    archived_entity = datastore.Entity(
        datastore_client.key(f'archived_{entity.kind}', entity.key.name),
    )

    archived_entity.update(entity.copy())
    archived_entity.exclude_from_indexes = entity.exclude_from_indexes

    return archived_entity


def unarchived_datastore_entity(entity, datastore_client):
    unarchived_entity = datastore.Entity(
        datastore_client.key(entity.kind.replace('archived_', ''), entity.key.name),
    )

    unarchived_entity.update(entity.copy())
    unarchived_entity.exclude_from_indexes = entity.exclude_from_indexes

    return unarchived_entity


def archive_datastore_entity(entity, datastore_client):
    datastore_client.put(archived_datastore_entity(entity, datastore_client))
    datastore_client.delete(entity.key)


def unarchive_datastore_entity(entity, datastore_client):
    datastore_client.put(unarchived_datastore_entity(entity, datastore_client))
    datastore_client.delete(entity.key)


# def archived_datastore_entity(hit_entity, datastore_client):
#     archived_hit = datastore.Entity(
#         datastore_client.key('archived_hit', hit_entity['hit_id']),
#     )
#
#     archived_hit.update(hit_entity.copy())
#     archived_hit.exclude_from_indexes = hit_entity.exclude_from_indexes
#
#     return archived_hit
#
#
# def archive_datastore_entity(hit_entity, datastore_client):
#     datastore_client.put(archived_datastore_entity(hit_entity, datastore_client))
#     datastore_client.delete(hit_entity.key)


def correct_question(question):
    question = question.strip()
    question = question[0].upper() + question[1:]
    if question[-1] != '?':
        question += '?'
    return question


def propagate_video_titles(hit_batches, active_hit_entities, inactive_hit_entities, datastore_client, production):
    print('\tpropagating video titles')

    video_title_hit_batch = hit_batches['video title']
    video_title_results = video_title_hit_batch.results

    video_titles_by_hit_id = {}

    for result in video_title_results:
        hit_id = result['hit_id']
        video_titles_by_hit_id[hit_id] = result['answers'][0]['video title']

    competition = CompetitionFFAWTA(match_size=3)

    for hit_id in video_titles_by_hit_id.keys():
        competition.add_contestant_by_id(hit_id)

    matchups = competition.get_matchups(num_matchups=NUM_VIDEO_TITLE_RATING_HITS)
    for matchup in matchups:
        video_title_list = [video_titles_by_hit_id[id] for id in matchup]

        launch_title_rating_hit_batch(
            submission_id=video_title_hit_batch.submission_id,
            video_titles=video_title_list,
            production=production,
            datastore_client=datastore_client,
            bar=False
        )

    return True, False, False


def propagate_title_ratings(hit_batches, active_hit_entities, inactive_hit_entities, datastore_client, production):
    print('\tpropagating title ratings')

    title_rating_hit_batch = hit_batches['title rating']

    competition = CompetitionFFAWTA(match_size=3)
    all_video_titles = set()

    for title_rating_result in title_rating_hit_batch.results:
        video_rating_hit_id = title_rating_result['hit_id']
        try:
            ratings = title_rating_result['answers'][0]
        except Exception:
            print('')

        video_rating_hit_entity = list(filter(lambda e: e['hit_id'] == video_rating_hit_id, active_hit_entities))[0]
        video_titles = video_rating_hit_entity['video_titles']
        for video_title in video_titles:
            all_video_titles.add(video_title)

        competition.add_contestants_by_ids(video_titles)

        competition.record_match(
            contestant_ids=video_titles,
            ranks=[int(ratings[f'rating_{i + 1}']) - 1 for i in range(len(video_titles))]
        )

    best_video_title = competition.best_id()

    launch_word_emphasis_hit_batch(submission_id=title_rating_hit_batch.submission_id, title=best_video_title, production=production,
                                   datastore_client=datastore_client)

    return True, False, False


def propagate_flavour_images(hit_batches, active_hit_entities, inactive_hit_entities, datastore_client, production):
    print('\tpropagating flavour images')

    hit_batch = hit_batches['flavour image']
    results = hit_batch.results
    image_locations = []
    for result in results:
        for answer in result['answers']:
            image_locations.append(answer['image location'])

    valid_image_locations = list(filter(is_image_and_ready, image_locations))
    invalid_image_locations = list(filter(lambda x: not is_image_and_ready(x), image_locations))

    # print('\n'.join(invalid_image_locations))
    # return False, False, False

    num_valid = len(valid_image_locations)
    num_invalid = NUM_FLAVOUR_IMAGE_HITS - num_valid

    print(f'\t {num_invalid} invalid and {num_valid} valid image locations received')

    if num_valid >= NUM_FLAVOUR_IMAGE_HITS:
        for image_location in valid_image_locations:
            launch_image_background_hit_batch(submission_id=hit_batch.submission_id, image_url=image_location, production=production,
                                              datastore_client=datastore_client)

        return True, False, False
    else:

        for _ in range(num_invalid):
            launch_flavour_image_hit_batch(submission_id=hit_batch.submission_id, production=production,
                                           datastore_client=datastore_client)
        return False, False, False


def has_solid_background(result):
    return result['answers'][0]['has_background'] and result['answers'][0]['background_solid']


def ready_thumbnails_for_rating(hit_batches, active_hit_entities, inactive_hit_entities, datastore_client, production):
    print('\treadying thumbnails for rating')

    image_background_hit_batch = hit_batches['image background']
    image_background_results = image_background_hit_batch.results

    word_emphasis_hit_entity = list(filter(lambda e: e['hit_type'] == 'word emphasis', active_hit_entities))[0]
    video_title = word_emphasis_hit_entity['title']

    word_emphasis_hit_batch = hit_batches['word emphasis']
    word_emphasis_results = word_emphasis_hit_batch.results

    thumbnails = {}
    submission = None

    for result in image_background_results:
        hit_id = result['hit_id']

        hit_entity = list(filter(lambda e: e['hit_id'] == hit_id, active_hit_entities))[0]
        image_location = hit_entity['image_location']

        if submission is None:
            submission = get_submission_by_id(hit_entity['submission_id'])

        emphasis_mask = word_emphasis_results[0]['answers'][0]['emphasis_mask']

        flavour_image = FlavourImage(
            image=image_from_url(image_location),
            has_solid_background=has_solid_background(result),
            background_coords=tuple(result['answers'][0]['background_coords']),
        )

        thumbnail = get_thumbnail(submission, video_title, color_scheme='dark', emphasis_mask=emphasis_mask, flavour_image=flavour_image, language_code='en')

        thumbnails[hit_id] = thumbnail

    storage_client = get_storage_client()
    bucket = storage_client.get_bucket('candidate_thumbnails')

    image_urls_by_hit_id = {}

    for hit_id, thumbnail in thumbnails.items():
        filename = hit_id + '.jpg'
        blob = bucket.blob(filename)

        with io.BytesIO() as output:
            thumbnail.save(output, format="JPEG")
            blob.upload_from_string(output.getvalue())
        blob.make_public()

        image_urls_by_hit_id[hit_id] = blob.public_url

        # b = bucket.get_blob(hit_id + '.jpg')
        # fp = io.BytesIO(b.download_as_string())
        # im = Image.open(fp)

    competition = CompetitionFFAWTA(match_size=3)

    for hit_id in image_urls_by_hit_id.keys():
        competition.add_contestant_by_id(hit_id)

    matchups = competition.get_matchups(num_matchups=NUM_THUMBNAIL_RATING_HITS)
    for matchup in matchups:
        image_urls_list = [image_urls_by_hit_id[id] for id in matchup]

        launch_thumbnail_rating_hit_batch(
            submission_id=image_background_hit_batch.submission_id,
            image_urls=image_urls_list,
            production=production,
            datastore_client=datastore_client,
            bar=False
        )

    return True, False, False


def finalize_thumbnail(hit_batches, active_hit_entities, inactive_hit_entities, datastore_client, production):
    print('\tfinalizing thumbnails')

    thumbnail_rating_hit_batch = hit_batches['thumbnail rating']

    competition = CompetitionFFAWTA(match_size=3)
    all_thumbnail_urls = set()

    for thumbnail_rating_result in thumbnail_rating_hit_batch.results:
        thumbnail_rating_hit_id = thumbnail_rating_result['hit_id']
        ratings = thumbnail_rating_result['answers'][0]

        thumbnail_rating_hit_entity = list(filter(lambda e: e['hit_id'] == thumbnail_rating_hit_id, active_hit_entities))[0]
        image_urls = thumbnail_rating_hit_entity['image_urls']
        for image_url in image_urls:
            all_thumbnail_urls.add(image_url)

        thumbnail_ids = [url.split('/')[-1].replace('.jpg', '') for url in image_urls]

        competition.add_contestants_by_ids(thumbnail_ids)

        competition.record_match(
            contestant_ids=thumbnail_ids,
            ranks=[int(ratings[f'rating_{i + 1}']) - 1 for i in range(len(thumbnail_ids))]
        )

    # move best flavour image to best_flavour_images bucket

    storage_client = get_storage_client()

    best_thumbnail_id = competition.best_id()

    try:
        image_background_hit_entity_for_best_flavour_image = list(filter(lambda e: e['hit_id'] == best_thumbnail_id, inactive_hit_entities))[0]
    except Exception:
        print()

    best_flavour_image_location = image_background_hit_entity_for_best_flavour_image['image_location']
    best_flavour_image = image_from_url(best_flavour_image_location).convert('RGB')

    best_flavour_images_bucket = storage_client.get_bucket('best_flavour_images')

    filename = thumbnail_rating_hit_batch.submission_id + '.jpg'

    best_flavour_image_blob = best_flavour_images_bucket.blob(filename)

    with io.BytesIO() as output:
        best_flavour_image.save(output, format="JPEG")
        best_flavour_image_blob.upload_from_string(output.getvalue())

    # create video formula
    image_background_result = image_background_hit_entity_for_best_flavour_image['results']

    if has_solid_background(image_background_result):
        background_coords = tuple(image_background_result['answers'][0]['background_coords'])
        background_color = list(best_flavour_image.getpixel(background_coords))
    else:
        background_color = None

    word_emphasis_hit_entity = list(filter(lambda e: e['hit_type'] == 'word emphasis', inactive_hit_entities))[0]
    video_title = word_emphasis_hit_entity['title']

    video_formula_entity = datastore.Entity(
        datastore_client.key('video_formula', thumbnail_rating_hit_batch.submission_id),
    )

    word_emphasis_hit_entity = list(filter(lambda e: e['hit_type'] == 'word emphasis', inactive_hit_entities))[0]
    emphasis_mask = word_emphasis_hit_entity['results']['answers'][0]['emphasis_mask']

    video_tags_hit_batch = hit_batches['video tags']
    additional_tags = video_tags_hit_batch.results[0]['answers'][0]['video tags']

    video_formula_entity.update(dict(
        submission_id=thumbnail_rating_hit_batch.submission_id,
        title=video_title,
        thumbnail_location=best_flavour_image_blob.name,
        has_solid_background=has_solid_background(image_background_result),
        background_color=background_color,
        emphasis_mask=emphasis_mask,
        creation_time=datetime.datetime.now(),
        additional_tags=additional_tags
    ))
    datastore_client.put(video_formula_entity)

    # delete other thumbnail blobs for submission

    candidate_thumbnails_bucket = storage_client.get_bucket('candidate_thumbnails')
    for thumbnail_url in all_thumbnail_urls:
        blob_name = thumbnail_url.split('/')[-1]
        blob = candidate_thumbnails_bucket.get_blob(blob_name)
        blob.delete()

    return True, False, True


propagation_logic = {
    (VideoTitleHitBatch,): propagate_video_titles,
    (TitleRatingHitBatch,): propagate_title_ratings,
    (FlavourImageHitBatch,): propagate_flavour_images,
    (ImageBackgroundHitBatch, WordEmphasisHitBatch): ready_thumbnails_for_rating,
    (ThumbnailRatingHitBatch, VideoTagsHitBatch): finalize_thumbnail,
}


# propagation_logic = {
#     (ShortenQuestionHitBatch,): propagate_shortened_question,
#     (FlavourImageHitBatch,): propagate_flavour_image,
#     (ImageBackgroundHitBatch, WordEmphasisHitBatch): propagate_image_background,
#     (ThumbnailRatingHitBatch,): finalize_thumbnail,
# }


class AlienAnswersHitPipeline:
    def __init__(self, submission_id, datastore_client=None, production=False):
        self.submission_id = submission_id.strip()
        self.datastore_client = get_datastore_client() if datastore_client is None else datastore_client
        self.production = production

    def launch(self):
        for _ in range(NUM_VIDEO_TITLE_HITS):
            launch_video_title_hit_batch(submission_id=self.submission_id, production=self.production, datastore_client=self.datastore_client)

        for _ in range(NUM_FLAVOUR_IMAGE_HITS):
            launch_flavour_image_hit_batch(submission_id=self.submission_id, production=self.production, datastore_client=self.datastore_client)

        launch_video_tags_hit_batch(submission_id=self.submission_id, production=self.production, datastore_client=self.datastore_client)

        # launch_word_emphasis_hit_batch(submission_id=self.submission_id, title='How can you even just?', production=self.production,
        #                                datastore_client=self.datastore_client)

        # launch_image_background_hit_batch(submission_id=self.submission_id,
        #                                   image_url='https://cdn.shopify.com/s/files/1/1234/1684/products/print_all_over_me_3_t-shirt_0000000p-one-true-god.jpg',
        #                                   production=self.production, datastore_client=self.datastore_client)
        # launch_image_background_hit_batch(submission_id=self.submission_id,
        #                                   image_url='https://www.coloursofistria.com/cms_media/images/ARTICLES/Umag-2015v.jpg',
        #                                   production=self.production, datastore_client=self.datastore_client)

    def create_aa_hit_batch_from_hit_ids(self, hb, hit_ids):
        hit_batch = create_aa_hit_batch_from_hit_ids(hb, hit_ids, self.submission_id, self.production, update=False)
        hit_batch.update_results(datastore_client=self.datastore_client)
        return hit_batch

    def archived_datastore_entity(self, hit_entity):
        return archived_datastore_entity(hit_entity, self.datastore_client)

    def archive_datastore_entity(self, hit_entity):
        archive_datastore_entity(hit_entity, self.datastore_client)

    def propagate(self):
        submission_id_query = self.datastore_client.query(kind='hit')
        submission_id_query.add_filter('submission_id', '=', self.submission_id)
        submission_id_query.add_filter('production', '=', self.production)
        submission_hit_entities = list(submission_id_query.fetch())

        active_hit_entities = list(filter(lambda e: e['active'], submission_hit_entities))
        inactive_hit_entities = list(filter(lambda e: not e['active'], submission_hit_entities))

        batch = self.datastore_client.batch()
        with batch:

            hit_batch_classes = []

            for preconditions in propagation_logic.keys():
                for hit_batch_class in preconditions:
                    hit_batch_classes.append(hit_batch_class)

            hit_batch_classes_by_hit_type = {hit_batch_class.hit_type: hit_batch_class for hit_batch_class in hit_batch_classes}
            ##

            hit_ids_by_hit_type = {hit_type: [] for hit_type in hit_batch_classes_by_hit_type.keys()}
            hit_entities_by_hit_type = {hit_type: [] for hit_type in hit_batch_classes_by_hit_type.keys()}

            for hit_entity in active_hit_entities:
                hit_id = hit_entity['hit_id']
                hit_type = hit_entity['hit_type']

                hit_ids_by_hit_type[hit_type].append(hit_id)
                hit_entities_by_hit_type[hit_type].append(hit_entity)

            # todo: make hit_batches a @dataclass
            hit_batches = {hit_type: self.create_aa_hit_batch_from_hit_ids(hit_batch_classes_by_hit_type[hit_type], hit_ids) for hit_type, hit_ids in
                           hit_ids_by_hit_type.items()}

            hit_batches_launched = {hit_type: hit_batch.assignments_launched > 0 for hit_type, hit_batch in hit_batches.items()}

            hit_batches_completed = {hit_type: hit_batch.completed() for hit_type, hit_batch in hit_batches.items()}

        batch = self.datastore_client.batch()
        with batch:
            for preconditions in propagation_logic.keys():
                # print([(hit_batch_class, hit_batches_completed[hit_batch_class.hit_type]) for hit_batch_class in preconditions])
                if all([hit_batches_completed[hit_batch_class.hit_type] for hit_batch_class in preconditions]):
                    deactivate, archive, final = propagation_logic[preconditions](hit_batches, active_hit_entities, inactive_hit_entities, self.datastore_client,
                                                                                  self.production)

                    if final:
                        # delete hit templates
                        boto_session = boto3.Session(profile_name=MTURK_PROFILE_NAME)
                        boto_client = boto_session.client(
                            service_name=MTURK_PROFILE_NAME,
                            region_name=MTURK_REGION_NAME,
                            endpoint_url=MTURK_ENVIRONMENTS["production"]['endpoint'] if self.production else MTURK_ENVIRONMENTS["sandbox"]['endpoint'],
                        )

                        for hit_entity in submission_hit_entities:
                            batch.put(self.archived_datastore_entity(hit_entity))
                            batch.delete(hit_entity.key)
                            boto_client.delete_hit(
                                HITId=hit_entity['hit_id']
                            )

                        # delete selected_post
                        query = self.datastore_client.query(kind='selected_post')
                        query.add_filter('submission_id', '=', self.submission_id)
                        self.datastore_client.delete(list(query.fetch())[0].key)

                        continue

                    if archive:
                        for hit_batch_class in preconditions:
                            for hit_entity in hit_entities_by_hit_type[hit_batch_class.hit_type]:
                                batch.put(self.archived_datastore_entity(hit_entity))
                                batch.delete(hit_entity.key)
                        continue

                    if deactivate:
                        for hit_batch_class in preconditions:
                            for hit_entity in hit_entities_by_hit_type[hit_batch_class.hit_type]:
                                current_hit_entity = self.datastore_client.get(hit_entity.key)
                                current_hit_entity.update(dict(active=False))
                                batch.put(current_hit_entity)
        pass


class AlienAnswersHitPipelineOrchestrator:
    def __init__(self, production=True, datastore_client=None):
        self.production = production
        self.datastore_client = get_datastore_client() if datastore_client is None else datastore_client

    def get_submission_ids_from_kind(self, kind):
        query = self.datastore_client.query(kind=kind)
        entities = list(query.fetch())
        return set([e['submission_id'] for e in entities])

    def get_selected_post_ids(self):
        return self.get_submission_ids_from_kind(kind='selected_post')

    def get_ids_in_pipeline(self):
        return self.get_submission_ids_from_kind(kind='hit')

    def get_num_video_formulas(self):
        return len(self.get_submission_ids_from_kind(kind='video_formula'))

    def get_num_ids_in_pipeline(self):
        return len(self.get_ids_in_pipeline())

    def get_pipelines_for_ids(self, submission_ids):
        pipelines = [AlienAnswersHitPipeline(submission_id=id, datastore_client=self.datastore_client, production=self.production) for id in submission_ids]
        random.shuffle(pipelines)
        return pipelines

    def propagate_ids(self, submission_ids):
        pipelines = self.get_pipelines_for_ids(submission_ids)
        self.propagate_pipelines(pipelines)

    def propagate_pipelines(self, pipelines):
        for pipeline in pipelines:
            print(f'propagating {pipeline.submission_id}')
            pipeline.propagate()
            self.print_line()

    def print_line(self):
        print('-' * 40)

    def launch_ids(self, ids):
        for pipeline in self.get_pipelines_for_ids(ids):
            print(f'launching {pipeline.submission_id}')
            pipeline.launch()
            self.print_line()

    def step(self):
        if self.get_num_video_formulas() < (NUM_ALL_TIME_TOP_POSTS_PER_DAY + NUM_WEEKLY_TOP_POSTS_PER_DAY) * 3:
            if self.get_num_ids_in_pipeline() == 0:
                print('selecting new posts')
                select_posts_for_week()

        print('launching new ids')
        self.print_line()
        unlaunched_ids = self.get_selected_post_ids() - self.get_ids_in_pipeline()
        self.launch_ids(unlaunched_ids)

        ids_in_pipeline = list(self.get_ids_in_pipeline())
        random.shuffle(ids_in_pipeline)

        print(f'propagating {len(ids_in_pipeline)} ids')
        self.print_line()

        self.propagate_ids(ids_in_pipeline)
        print('propagation complete\n')

    def loop(self, sleep_duration=600):
        while True:
            self.step()
            print('\nsleeping\n')
            time.sleep(sleep_duration)
            # tqdm_countdown(secs=60 * 10, description='sleeping')
            print()


if __name__ == "__main__":
    # dc = get_datastore_client()
    # p = AlienAnswersHitPipeline(submission_id='2vpng7', datastore_client=dc, production=False)
    # p.launch()
    # p.propagate()

    # u = 'https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcQp9RDt3dlUL4OAegbpTr0lSU3P61_LcavmYN_lPbZVCaSkQw_1&s'
    # print()

    AlienAnswersHitPipelineOrchestrator().loop()

    # ids = ['dcepx9', 'dcduwk', '5ipinn', '55ng8w', '348vlx', '2vpng7', '2np694']
    # ids = get_selected_post_ids()
    # test_run(ids, production=True)  # todo add id check to launch
    # propagation_loop(ids, production=True)

    # while True:
    #     try:
    #         propagation_loop(ids, production=True)
    #     except Exception as e:
    #         print(e)
    #         random.shuffle(ids)
    #         time.sleep(60 * 5)

    # ids = ['ddh8vm']
    # test_run(ids, production=False)
    # propagation_loop(ids, production=False)

    # select_posts_for_week()

    'https://cdn.shopify.com/s/files/1/1234/1684/products/print_all_over_me_3_t-shirt_0000000p-one-true-god.jpg'
    'https://i.ytimg.com/vi/iAeYPfrXwk4/maxresdefault.jpg'
    'https://art.pixilart.com/thumb/8a04033f59cd6c7.png'

    'https://cdn.arstechnica.net/wp-content/uploads/2016/02/5718897981_10faa45ac3_b-640x624.jpg'
    'https://upload.wikimedia.org/wikipedia/commons/9/9a/Gull_portrait_ca_usa.jpg'
    'https://media.wired.com/photos/5c1ae77ae91b067f6d57dec0/master/pass/Comparison-City-MAIN-ART.jpg'
