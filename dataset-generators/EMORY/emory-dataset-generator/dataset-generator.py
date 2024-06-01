"""
This code is not optimal
"""

import pandas as pd
import numpy as np
import re
import json
from tqdm import tqdm

embed_clinical = pd.read_csv("../emory-patient-feats/tables/EMBED_OpenData_clinical.csv", low_memory=False)
embed_metadata = pd.read_csv("../emory-patient-feats/tables/EMBED_OpenData_metadata_reduced.csv", low_memory=False)
embed_metadata_reduced = pd.read_csv("../emory-patient-feats/tables/EMBED_OpenData_metadata.csv", low_memory=False)

embed_clinical.fillna("N/A", inplace=True)
embed_metadata.fillna("N/A", inplace=True)
embed_metadata_reduced.fillna("N/A", inplace=True)


incompat_race_desc = ["Patient Declines", "Unknown, Unavailable or Unreported", "Not Recorded", "N/A"]
incompat_marital_desc = ["Unknown", "Not Recorded", "N/A"]

all_qs = []

bi_rads_openend_vals = {
    "A": ["BIRADS 0", "additional evaluation required"],
    "N": ["BIRADS 1", "negative"],
    "B": ["BIRADS 2","benign"],
    "P": ["BIRADS 3","probably benign"],
    "S": ["BIRADS 4","suspicious"],
    "M": ["BIRADS 5","highly suggestive of malignancy"],
}

bi_rads_to_mcq = {
    "A": "A",
    "N": "B",
    "B": "C",
    "P": "D",
    "S": "E",
    "M": "F",
}


tissue_den_vals_to_mcq = {
    1: "A",
    2: "B",
    3: "C",
    4: "D",
    5: "E",
}

tissue_den_vals_to_open_end = {
    1: " entirely fat",
    2: " scattered fibroglandular",
    3: " heterogenously dense",
    4: " extremely dense",
    5: " male equivalent",
}

view_abrev_to_term = {
    "CC": "craniocaudal",
    "MLO": "mediolateral oblique",
}

path_sev_to_mcq = {
    0: "A",
    1: "B",
    2: "C",
    3: "D",
    4: "E",
    5: "F",
}

path_sev_to_open_end = {
    0: "invasive cancer",
    1: "non-invasive cancer",
    2: "high-risk lesion",
    3: "borderline lesion",
    4: "benign findings",
    5: "normal findings"
}


for _, curr_pat_row in tqdm(embed_clinical.iterrows(), total=len(embed_clinical)):
    curr_pat_stud_num = curr_pat_row["acc_anon"]
    curr_study_df = embed_metadata.loc[embed_metadata["acc_anon"] == curr_pat_stud_num]

    # Base information generation        
    race = ""

    if curr_pat_row["ETHNIC_GROUP_DESC"] not in incompat_race_desc:
        race_desc = curr_pat_row["ETHNIC_GROUP_DESC"]
        if race_desc == "African American  or Black":
            race = "african american"
        elif race == "American Indian or Alaskan Native":
            race == "native american"
        elif race == "Asian":
            race = "asian"
        elif race == "Caucasian or White":
            race == "white"
        elif race == "Multiple":
            race = "mixed"
        elif race == "Native Hawaiian or Other Pacific Islander":
            race == "pacific islander"

    marital_status = ""

    if curr_pat_row["MARITAL_STATUS_DESC"] not in incompat_marital_desc:
        marital_status = curr_pat_row["MARITAL_STATUS_DESC"].lower()

    gender = curr_pat_row["GENDER_DESC"].lower() if curr_pat_row["GENDER_DESC"] != "N/A" else ""
    age = int(curr_pat_row["age_at_study"]) if curr_pat_row["age_at_study"] != "N/A" else ""
    desc = "diagnostic" if "Diagnostic" in curr_pat_row["desc"] else "screen"

    base = re.sub(r' +', ' ', f"A {marital_status} {gender} {race} {'who is ' if age != 'N/A' else ''} {age} has the following {desc} mammogram result.") 
    curr_study_img_paths = embed_metadata_reduced.loc[embed_metadata_reduced["acc_anon"] == curr_pat_stud_num]["anon_dicom_path"].to_list()

    # BIRADS assessment score question
    if(curr_pat_row.loc["asses"] != "X" and curr_pat_row.loc["asses"] != "K"):           
        bi_rads_assessment_q =  f"{base} What is your BIRADS assessment score?"        
        bi_rads_mcq_version = bi_rads_assessment_q + "\n Choices: (A) BIRADS 0 (B) BIRADS 1 (C) BIRADS 2 (D) BIRADS 3 (E) BIRADS 4 (F) BIRADS 5"
        bi_rads_mcq_ans = bi_rads_to_mcq[curr_pat_row.loc["asses"]]
        
        bi_rads_open_end_ans = f"The assessment score is {bi_rads_openend_vals[curr_pat_row.loc['asses']][0]}, meaning {bi_rads_openend_vals[curr_pat_row.loc['asses']][1]}."

        bi_rads_q = {
            "img_paths": curr_study_img_paths,
            "mcq":{
                "q": bi_rads_mcq_version,
                "a": bi_rads_mcq_ans},
            "open_end": {
                "q": bi_rads_assessment_q,
                "a": bi_rads_open_end_ans
            },
            "qtype": "asses"
        }

        all_qs.append(bi_rads_q)

    # Path Severity Assessment question
    if(curr_pat_row["path_severity"] != "N/A"):
        path_sev_q = f"{base} What is the path severity of the mammogram?"
        path_sev_mcq_version = path_sev_q + "\n Choices: (A) invasive cancer (B) non-invasive cancer (C) high-risk lesion (D) borderline lesion (E) benign findings (F) normal findings"
        path_sev_mcq_ans = curr_pat_row["path_severity"]
        path_sev_open_end_ans = f"The path severity assessment indicated {path_sev_to_open_end[int(curr_pat_row['path_severity'])]}."
        
        path_sev_qa = {
            "img_paths": curr_study_img_paths,
            "mcq":{
                "q": path_sev_mcq_version,
                "a": path_sev_to_mcq[int(curr_pat_row["path_severity"])]},
            "open_end": {
                "q": path_sev_q,
                "a": path_sev_open_end_ans
            },
            "qtype": "path_sev"
        }

        all_qs.append(path_sev_qa)
        
    for _, curr_study_row in curr_study_df.iterrows():
        # Tissue Density Question
        if(curr_pat_row["tissueden"] != "N/A"):
            tissue_den_q = f"{base} What is the tissue density of the mammogram?"
            tissue_den_mcq_version = tissue_den_q + "\nChoices: (A) entirely fat (B) scattered fibroglandular (C) heterogenously dense  (D) extremely dense (E) male equivalent"
            tissue_den_mcq_ans = tissue_den_vals_to_mcq[int(curr_pat_row["tissueden"])]
            tissue_den_open_end_ans = f"The breast appear to be {tissue_den_vals_to_open_end[int(curr_pat_row['tissueden'])]}"
            img_paths = [curr_study_row["anon_dicom_path"]]
            
            tissue_den_qa = {
                "img_paths": img_paths,
                "mcq":{
                    "q": tissue_den_mcq_version,
                    "a": tissue_den_mcq_ans},
                "open_end": {
                    "q": tissue_den_q,
                    "a": tissue_den_open_end_ans
                },
                "qtype": "density"
            }

            all_qs.append(tissue_den_qa)

        # View Position
        if(curr_study_row["ViewPosition"] == "CC" or curr_study_row["ViewPosition"] == "MLO"):
            view_den_q  = "What is the view position of this mammogram?"
            view_den_mcq_version = f"{view_den_q} \nChoices: (A) CC (B) MLO"
            view_den_mcq_ans = "A" if curr_study_row["ViewPosition"] == "CC" else "B"
            view_den_open_end_ans = f"The breast appear to be {view_abrev_to_term[curr_study_row['ViewPosition']]}."
            img_paths = [curr_study_row["anon_dicom_path"]]

            view_den_qa = {
                "img_paths": img_paths,
                "mcq":{
                    "q": view_den_mcq_version,
                    "a": view_den_mcq_ans},
                "open_end": {
                    "q": view_den_q,
                    "a": view_den_open_end_ans
                },
                "qtype": "view"
            }

            all_qs.append(view_den_qa)
        

        # Age
        if(curr_pat_row["age_at_study"] != "N/A"):
            age_q = f"What is the age of the patient?"

            first_option = np.random.randint(17, 40)
            second_option = np.random.randint(40, 50)
            third_option = np.random.randint(50, 60)
            fourth_option = np.random.randint(70, 80)
            fifth_option = np.random.randint(80, 90)
            
            age_mcq_ans = ""
            
            if(int(curr_pat_row["age_at_study"]) < 40):
                first_option = int(curr_pat_row["age_at_study"])
                age_mcq_ans = "A"
            elif(int(curr_pat_row["age_at_study"]) < 50):
                second_option = int(curr_pat_row["age_at_study"])
                age_mcq_ans = "B"
            elif(int(curr_pat_row["age_at_study"]) < 60):
                third_option = int(curr_pat_row["age_at_study"])
                age_mcq_ans = "C"
            elif(int(curr_pat_row["age_at_study"]) < 70):
                fourth_option = int(curr_pat_row["age_at_study"])
                age_mcq_ans = "D"
            else:
                fifth_option = int(curr_pat_row["age_at_study"])
                age_mcq_ans = "E"


            age_mcq_version = f"{age_q} \nChoices: (A) {first_option} (B) {second_option} (C) {third_option} (D) {fourth_option} (E) {fifth_option}"
            age_open_end_ans = f"The patient is about {int(curr_pat_row['age_at_study'])} years of age."
            img_paths = [curr_study_row["anon_dicom_path"]]

            age_qa = {"img_paths": img_paths,
                "mcq":{
                    "q": age_mcq_version,
                    "a": age_mcq_ans},
                "open_end": {
                    "q": age_q,
                    "a": age_open_end_ans
                },
                "qtype": "age"
            }

            all_qs.append(age_qa)

        # ROI annotations
        if(curr_pat_row["roi_annotations"] != "N/A" )



qs = {"qs": all_qs}

with open("qs.json", "w") as f:
    json.dump(qs, f)