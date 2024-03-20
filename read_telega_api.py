
import datetime
from sys import getsizeof
import os
from typing import List, Dict
import logging

import pandas as pd
from dotenv import load_dotenv
from telethon.sync import TelegramClient

partitioning_column = 'msg_month_key'

logging.basicConfig(level=logging.INFO)
load_dotenv()
api_id = os.getenv('TELEGRAM_API_ID')
api_hash = os.getenv('TELEGRAM_API_HASH')

client = TelegramClient('anon', api_id, api_hash, system_version='4.16.30-vxDKLMN')
# need to specify any value for system_version, othervise you'll loose all you other sessions on all devices
# https://github.com/LonamiWebs/Telethon/issues/4051


def __extract_message_data(msg) -> Dict:
    col_maps = {
        ("msg_id", lambda mes: mes.id),
        ("user_id", lambda mes: mes.from_id.user_id),
        ("user_name", lambda mes: mes.sender.first_name
         if msg.sender else None),
        ("msg_date", lambda msg: msg.date),
        (partitioning_column, lambda msg: msg.date.year*100 + msg.date.month),  # key for partitioning
        ("reply_to_msg_id", lambda msg: msg.reply_to.reply_to_msg_id
            if msg.reply_to and msg.reply_to.reply_to_msg_id else None),
        ("msg_text", lambda msg: msg.message),
        ("msg_len", lambda msg: len(msg.message)),
        ("is_question", lambda msg: '?' in msg.message),
        ("react_cnt", lambda msg: len(msg.reactions.results)
         if msg.reactions and msg.reactions.results else 0),
    }
    mess_data = {}
    for atr, fnc in col_maps:
        mess_data[atr] = fnc(msg)
    return mess_data


def extract_messages(chat_id: int, limit: int = 1000, min_id: int = 0) -> List[Dict]:
    ret = []
    msg_dt, cnt_by_dt, cnt_all = None, 0, 0
    logging.info(f'started at {datetime.datetime.now()}')
    with client:
        for msg in client.iter_messages(chat_id,  wait_time=1, limit=limit, min_id=min_id):
            if not msg.from_id or not msg.message:
                continue
            if msg.date.date() != msg_dt and msg_dt:
                logging.info(f"got {cnt_by_dt=}/{cnt_all=} messages for {msg_dt}. total size in memory is {getsizeof(ret)} bytes")
                cnt_by_dt = 0
            d_i = __extract_message_data(msg)
            msg_dt = msg.date.date()
            cnt_all += 1
            cnt_by_dt += 1
            ret.append(d_i)
    logging.info(f"got {cnt_by_dt} messages for {msg_dt}. total size in memory is {getsizeof(ret)} bytes")
    return ret


def extract_to_parquet(tlg_group_id: int):    
    xx = extract_messages(chat_id=tlg_group_id, limit=1000)
    df = pd.DataFrame.from_dict(xx)
    file_name = f'chat{tlg_group_id}.parquet.gzip'
    df.to_parquet(file_name, compression='gzip', engine='fastparquet')
    # , partition_cols=[partitioning_column]
    # print(xx)


def append_to_parquet(tlg_group_id: int):
    file_name = f'chat{tlg_group_id}.parquet.gzip'
    id_col = 'msg_id'
    df = pd.read_parquet(file_name, columns=[id_col])
    max_id = df[id_col].max()
    new_msgs = extract_messages(chat_id=tlg_group_id, limit=1000000, min_id=max_id+1)
    if new_msgs:
        df = pd.DataFrame.from_dict(new_msgs)
        print(f'{df.shape=}')
        df.to_parquet(file_name, engine='fastparquet', append=True)


if __name__ == "__main__":
    tlg_group_id = -1001688539638
    extract_to_parquet(tlg_group_id)


# %%
