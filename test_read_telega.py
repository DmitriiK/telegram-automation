from read_telega_api import extract_to_parquet, extract_participants, dump_participants
import json
import pandas as pd


def test_load_json():
    dump_file = r'data\participants.json'
    df = pd.read_json(dump_file)
    print(df.head())

       
if __name__ == "__main__":
    tlg_group_id = -1001688539638
    file_path = rf'data\chat{tlg_group_id}.parquet.gzip'
    # extract_to_parquet(tlg_group_id, file_path)
    # extract_to_parquet(tlg_group_id, file_path, True)
    #dump_participants(tlg_group_id, r'data\participants.json')
    test_load_json()
# print(xx)


# %%
