"""
This code is not optimal
"""

import pandas as pd
import numpy as np
import re
import json
import ast
from tqdm import tqdm
from collections import Counter


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

pos_locs = np.array([["top left", "top center", "top right"], 
                     ["middle left", "middle center", "middle right"], 
                     ["bottom left", "bottom center", "bottom right"]])

pos_locs_sample = pos_locs.flatten()

pos_correct_idx_to_letter = {
    0: "A",
    1: "B",
    2: "C",
    3: "D",
    4: "E",
    5: "F",
}

def extract_unique_and_counts(sequences_array):
    unique_values_list = []
    counts_list = []

    for sequence in sequences_array:
        value_counts = Counter(sequence)

        # Extract unique values and counts as NumPy arrays
        unique_values = np.array(list(value_counts.keys()))
        counts = np.array(list(value_counts.values()))

        unique_values_list.append(unique_values)
        counts_list.append(counts)

    return unique_values_list, counts_list

for _, curr_pat_row in tqdm(embed_clinical.iterrows(), total=len(embed_clinical)):
    curr_pat_stud_num = curr_pat_row["acc_anon"]
    curr_study_df = embed_metadata_reduced.loc[embed_metadata["acc_anon"] == curr_pat_stud_num]

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
        if(curr_study_row["ROI_coords"] != "N/A" and
           curr_study_row["ROI_coords"] != "()" and 
           curr_study_row["WindowWidth"] != "N/A" and 
           curr_study_row["WindowCenter"] != "N/A" and
           len(curr_study_row["WindowCenter"]) < 7 and
           len(curr_study_row["WindowWidth"]) < 7):

            width = float(curr_study_row["WindowWidth"])
            length = float(curr_study_row["WindowCenter"])
            rois = [[float(x) for x in re.findall(r"-?\d+", sublist)] for sublist in re.findall(r"\([^()]+\)", curr_study_row["ROI_coords"])]

            locs = []

            for roi in rois:
                center_loc_x = (roi[-1] - roi[1]) / 2
                center_loc_y = (roi[2] - roi[0]) / 2

                percent_width = (center_loc_x + roi[1]) / width
                percent_len = (center_loc_y + roi[0]) / length

                ans = ""

                if percent_width < 0.33 and percent_len < 0.33:
                    ans = pos_locs[0, 0]
                elif percent_width < 0.66 and percent_len < 0.33:
                    ans = pos_locs[0, 1]
                elif percent_width < 1 and percent_len < 0.33:
                    ans = pos_locs[0, 2]
                elif percent_width < 0.33 and percent_len < 0.66:
                    ans = pos_locs[1, 0]
                elif percent_width < 0.66 and percent_len < 0.66:
                    ans = pos_locs[1, 1]
                elif percent_width < 1 and percent_len < 0.66:
                    ans = pos_locs[1, 2]
                elif percent_width < 0.33 and percent_len < 1:
                    ans = pos_locs[2, 0]
                elif percent_width < 0.66 and percent_len < 1:
                    ans = pos_locs[2, 1]
                else:
                    ans = pos_locs[2, 2]

                locs.append(ans)

            locs = np.array([locs])

            true_uni_locs, true_counts = extract_unique_and_counts(locs) 

            mcq_false_ops = np.random.choice(pos_locs_sample, size=(4, len(locs)), replace=True)
            ops_uni_locs, ops_counts = extract_unique_and_counts(mcq_false_ops) 

            ins_idx = np.random.randint(0, 4)

            ops_uni_locs.insert(ins_idx, true_uni_locs[0]) 
            ops_counts.insert(ins_idx, true_counts[0])

            loc_mcq_ans_choices = []

            for curr_loc, curr_count in zip(ops_uni_locs, ops_counts):
                curr_str = ""
                
                for idx, (loc_val, count_val) in enumerate(zip(curr_loc, curr_count)): 
                    if idx != len(curr_loc) - 1:
                        curr_str += f"{count_val} {loc_val}, "
                    else:
                        curr_str += f"{count_val} {loc_val}"
                                    
                loc_mcq_ans_choices.append(curr_str)
            
            loc_q = f"{base} What are the locations of the ROIs?"
            loc_mcq_version = f"{loc_q} \nChoices: (A) {loc_mcq_ans_choices[0]} (B) {loc_mcq_ans_choices[1]} (C) {loc_mcq_ans_choices[2]} (D) {loc_mcq_ans_choices[3]} (E) {loc_mcq_ans_choices[4]}"
            loc_mcq_ans = pos_correct_idx_to_letter[ins_idx]

            loc_open_end_ans = f"For ROIs, there are {loc_mcq_ans_choices[ins_idx]}."

            img_paths = [curr_study_row["anon_dicom_path"]]

            loc_qa = {
                "img_paths": img_paths,
                "mcq":{
                    "q": loc_mcq_version,
                    "a": loc_mcq_ans},
                "open_end": {
                    "q": loc_q,
                    "a": loc_open_end_ans
                },
                "qtype": "loc"
            }

            all_qs.append(loc_qa)


qs = {"qs": all_qs}

with open("qs.json", "w") as f:
    json.dump(qs, f)