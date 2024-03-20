from read_telega_api import extract_to_parquet
if __name__ == "__main__":
    tlg_group_id = -1001688539638
    file_path = rf'data\chat{tlg_group_id}.parquet.gzip'
    extract_to_parquet(tlg_group_id, file_path)
    extract_to_parquet(tlg_group_id, file_path, True)


# %%
