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