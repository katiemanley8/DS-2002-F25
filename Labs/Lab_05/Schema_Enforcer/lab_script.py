# Lab Script for Lab 5
# All code for Parts 1 and 2 goes here.

# Task 1
import csv
data = [["student_id", "major", "GPA", "is_cs_major", "credits_taken"],
        [101, "Computer Science", 3.5, "Yes", "12.0"],
        [102, "Mathematics", 4, "No", "15.5"],
        ["103", "Physics", 2.9, "Yes", "14.0"],
        [104, 202, 3.2, "No", "13.0"],
        [105, "Biology", "3.1", "Yes", "ten"]]
with open("raw_survey_data.csv", mode="w", newline="") as file:
    writer = csv.writer(file)
    writer.writerows(data)
print("File 'raw_survey_data.csv' created with intentional type mismatches.")

# Task 2
import json
courses = [
    {
        "course_id": "DS2002",
        "section": "001",
        "title": "Data Science Systems",
        "level": 200,
        "instructors": [
            {"name": "Austin Rivera", "role": "Primary"},
            {"name": "Heywood Williams-Tracy", "role": "TA"}
        ]
    },
    {
        "course_id": "PSYC5500",
        "section": "001",
        "title": "Affective Neuroscience",
        "level": 500,
        "instructors": [
            {"name": "James Coan", "role": "Primary"}
        ]
    },
    {
        "course_id": "STAT3080",
        "section": "002",
        "title": "Data Visualization",
        "level": 300,
        "instructors": [
            {"name": "Rich Ross", "role": "Primary"}
        ]
    }
]
with open("raw_course_catalog.json", "w") as json_file:
    json.dump(courses, json_file, indent=2)
print("File 'raw_course_catalog.json' created with course data.")

# Task 3
import pandas as pd
df = pd.read_csv("raw_survey_data.csv")
df['is_cs_major'] = df['is_cs_major'].replace({'Yes': True, 'No': False})
df = df.astype({
    'GPA': 'float64',
    'credits_taken': 'float64',
    'student_id': 'int64',
    'major': 'string'
}, errors='ignore')
df.to_csv("clean_survey_data.csv", index=False)
print("Cleaned data saved to 'clean_survey_data.csv'.")

# Task 4
import json
import pandas as pd
with open("raw_course_catalog.json", "r") as json_file:
    courses = json.load(json_file)
normalized_courses = pd.json_normalize(courses, record_path=['instructors'],
                                       meta=['course_id', 'title', 'level', 'section'],
                                       errors='ignore')
normalized_courses.to_csv("clean_course_catalog.csv", index=False)
print("Normalized course data saved to 'clean_course_catalog.csv'.")
