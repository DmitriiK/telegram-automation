
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
# need to specify any value for system_version, othervise you'll loose all your other sessions on all devices
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


def extract_participants(chat_id: int) -> List[Dict]:
    atrs = [  # attributes to be extracted
            ('id', lambda x: x.id),
            ('username', lambda x: x.username),
            ('first_name', lambda x: x.first_name)
            ]
    with client:
        ret = [{atr[0]: atr[1](usr) for atr in atrs}
               for usr in client.iter_participants(chat_id)]
        logging.info(f'extracted {len(ret)} users.')
    return ret


def dump_participants(tlg_group_id: int, file_path: str):
    dd = extract_participants(tlg_group_id)
    with open(file_path, "w",  encoding="utf-8") as f:
        strs = '\n'.join([str(x) for x in dd])
        final = f'"chat_id": {tlg_group_id},\n "messages": [{strs}]'
        f.write(final)

def extract_messages(chat_id: int,  min_id: int = 0) -> List[Dict]:
    ret = []
    msg_dt, cnt_by_dt, cnt_all = None, 0, 0
    logging.info(f'started at {datetime.datetime.now()}')
    with client:
        for msg in client.iter_messages(chat_id,  wait_time=1,  min_id=min_id, limit=500000):
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


def lst_of_dict_to_parquet(di_itms: List[Dict], file_path: str, append: bool = True):
    df = pd.DataFrame.from_dict(di_itms)
    df.to_parquet(file_path, compression='gzip', engine='fastparquet', partition_cols=[partitioning_column],
                  append=append)


def extract_to_parquet(tlg_group_id: int, file_path: str, is_incremental: bool = False):
    max_id = 0  # max existing id in file
    if is_incremental:
        max_id = check_max_id(file_path)+1
        logging.info(f'extracting messages from  {max_id=} ')
    di_itms = extract_messages(chat_id=tlg_group_id,  min_id=max_id)
    logging.info(f'extracted {len(di_itms)} messages')
    if di_itms:
        lst_of_dict_to_parquet(di_itms, file_path, append=is_incremental)


def check_max_id(file_path: str):
    id_col = 'msg_id'
    df = pd.read_parquet(file_path, columns=[id_col])
    return df[id_col].max()
