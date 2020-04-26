MINUTE = 60
HOUR = 60 * MINUTE

# all given in seconds
DEFAULT_HIT_LIFETIME = HOUR * 24 * 3
DEFAULT_APPROVAL_DELAY = HOUR * 3

DEFAULT_REWARD_PER_MINUTE = 0.02
_DEFAULT_REWARD_PER_SECOND = DEFAULT_REWARD_PER_MINUTE / 60

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
MTURK_REGION_NAME = 'us-east-1'

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
adult_qualification = {
    'QualificationTypeId': "00000000000000000060",  # ok with adult  content
    'Comparator': "EqualTo",
    'IntegerValues': [1],
    'ActionsGuarded': "DiscoverPreviewAndAccept",
}


def generate_task_qualifications(adult=False, approval_rate=0.97, num_approved_hits=100, from_america=True, high_school=False):
    task_attributes = [
        {
            'QualificationTypeId': "000000000000000000L0",  # percentage approved
            'Comparator': "GreaterThan",
            'IntegerValues': [int(approval_rate * 100)],
            'ActionsGuarded': "DiscoverPreviewAndAccept",
        },
        {
            'QualificationTypeId': "00000000000000000040",  # Worker_​NumberHITsApproved
            'Comparator': "GreaterThan",
            'IntegerValues': [num_approved_hits],
            'ActionsGuarded': "DiscoverPreviewAndAccept",
        },
        from_america_qualification,
    ]

    assert not(high_school and not from_america)

    if adult:
        task_attributes.append(adult_qualification)
    if from_america:
        task_attributes.append(from_america_qualification)
    if high_school:
        task_attributes.append(us_high_school_qualification)

    return task_attributes


DEFAULT_TASK_QUALIFICATIONS = generate_task_qualifications()

mandatory_hit_attributes = {
    'Title',
    'Keywords',
    'Description',
    'AssignmentDurationInSeconds',
    'LifetimeInSeconds',
    'Reward',
    'QualificationRequirements',
    # 'MaxAssignments' # given at launch time
}