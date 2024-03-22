from read_telega_api import extract_to_parquet, extract_participants, dump_participants
import json


def test_load_json():
    with open(r'data\participats.json', 'r') as openfile:
        return json.load(openfile)

       
if __name__ == "__main__":
    tlg_group_id = -1001688539638
    file_path = rf'data\chat{tlg_group_id}.parquet.gzip'
    # extract_to_parquet(tlg_group_id, file_path)
    # extract_to_parquet(tlg_group_id, file_path, True)
    dump_participants(tlg_group_id, r'data\participats.json')
# print(xx)


# %%
