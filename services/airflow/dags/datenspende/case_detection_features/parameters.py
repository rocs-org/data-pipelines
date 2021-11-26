WEEKLY_QUESTIONNAIRE = 2
ONE_OFF_QUESTIONNAIRE = 10

ONE_OFF_QUESTIONS = {
    "test_result": 129,  # True/False/NULL
    "symptoms": 86,  # True/False/NULL
    "age": 133,  # INT/NULL
    "sex": 127,  # STR/NULL
    "height": 74,  # INT/NULL
    "fitness": 76,
    "weight": 75,  # INT/NULL
}
WEEKLY_QUESTIONS = {
    "test_result": 10,  # True/False/NULL
    "symptoms": 8,
    "age": 133,
    "sex": 127,
    "height": 74,
    "fitness": 76,
    "weight": 75,
}
WEEKLY_FEATURE_EXTRACTION_ARGS = [
    WEEKLY_QUESTIONNAIRE,
    WEEKLY_QUESTIONS,
    "homogenized_features",
    "homogenized_features_description",
]
ONE_OFF_FEATURE_EXTRACTION_ARGS = [
    ONE_OFF_QUESTIONNAIRE,
    ONE_OFF_QUESTIONS,
    "homogenized_features",
    "homogenized_features_description",
]

# map feature ids between one off survey and weekly survey
FEATURE_MAPPING = {
    451: 40,  # Fieber
    452: 40,  # Schüttelfrost
    453: 44,  # Husten
    470: 47,  # Halsschmerzen
    471: 45,  # Schupfen
    472: 48,  # Kopfschmerzen
    473: 41,  # Gliederschmerzen
    475: 46,  # Durchfall
    479: 49,  # Keine
    476: 42,  # Geruchs und
    477: 42,  # Geschmacksverlust
    129: 10,  # Testergebniss
}
