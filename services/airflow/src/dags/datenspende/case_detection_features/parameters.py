WEEKLY_QUESTIONNAIRE = 2
ONE_OFF_QUESTIONNAIRE = 10

ONE_OFF_QUESTIONS = {
    "test_result": 129,  # True/False/NULL
    "vaccination_status": 121,  # True/False/NULL
    "test_type": 81,  # 547: PRC, 548: Antigen, 549: Antibody, 550: Unknown
    "symptoms": [86],  # True/False/NULL
    "age": 133,  # INT/NULL
    "sex": 127,  # STR/NULL
    "height": 74,  # INT/NULL
    "fitness": 76,
    "weight": 75,  # INT/NULL
}
WEEKLY_QUESTIONS = {
    "test_result": 10,  # True/False/NULL
    "test_type": 91,  # 547: PRC, 548: Antigen, 549: Antibody, 550: Unknown
    "vaccination_status": 30,  # True/False/NULL
    "symptoms": [8, 137],
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
    81: 91,  # Test type
    30: 121,  # Vaccination status
    129: 10,  # Test result
    452: 40,  # Schüttelfrost
    473: 41,  # Gliederschmerzen
    476: 42,  # Geruchs und
    477: 42,  # Geschmacksverlust
    453: 44,  # Husten
    471: 45,  # Schupfen
    475: 46,  # Durchfall
    470: 47,  # Halsschmerzen
    472: 48,  # Kopfschmerzen
    479: 49,  # Keine
    # Answers from ID 137
    857: 40,  # Schüttelfrost
    858: 41,  # Gliederschmerzen
    859: 42,  # Geruchs und Geschmacksverlust
    860: 43,  # Fatigue
    861: 44,  # Husten
    862: 45,  # Schupfen
    863: 46,  # Durchfall
    864: 47,  # Halsschmerzen
    865: 48,  # Kopfschmerzen
    866: 451,  # Fever
    867: 467,  # Short of breath
    868: 49,  # Keine
}
